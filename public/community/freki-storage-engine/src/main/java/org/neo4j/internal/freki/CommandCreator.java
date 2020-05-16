/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.OptionalLong;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.freki.FrekiCommand.Mode;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.api.NamedToken;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.MutableNodeData.internalRelationshipIdFromRelationshipId;
import static org.neo4j.internal.helpers.collection.Iterators.loop;

public class CommandCreator implements TxStateVisitor
{
    private final Collection<StorageCommand> commands;
    private final Stores stores;
    private final ConstraintRuleAccessor constraintSemantics;
    private final PageCursorTracer cursorTracer;
    private final GraphUpdates graphUpdates;

    public CommandCreator( Collection<StorageCommand> commands, Stores stores, ConstraintRuleAccessor constraintSemantics, PageCursorTracer cursorTracer,
                           MemoryTracker memoryTracker )
    {
        this( commands, stores, constraintSemantics, cursorTracer, memoryTracker, false);
    }
    public CommandCreator( Collection<StorageCommand> commands, Stores stores, ConstraintRuleAccessor constraintSemantics, PageCursorTracer cursorTracer,
            MemoryTracker memoryTracker, boolean createOnly )
    {
        this.commands = commands;
        this.stores = stores;
        this.constraintSemantics = constraintSemantics;
        this.cursorTracer = cursorTracer;
        this.graphUpdates = new GraphUpdates( stores, cursorTracer, memoryTracker );
        if (createOnly)
            graphUpdates.setOnlyCreateMode( createOnly );
    }

    @Override
    public void visitCreatedNode( long id )
    {
        graphUpdates.create( id );
    }

    @Override
    public void visitDeletedNode( long id )
    {
        graphUpdates.getOrLoad( id ).delete();
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode, Iterable<StorageProperty> addedProperties )
    {
        long internalRelationshipId = internalRelationshipIdFromRelationshipId( id );
        createRelationship( internalRelationshipId, type, startNode, endNode, true, addedProperties );
        if ( startNode != endNode )
        {
            createRelationship( internalRelationshipId, type, endNode, startNode, false, addedProperties );
        }
    }

    private void createRelationship( long internalRelationshipId, int type, long sourceNode, long targetNode, boolean outgoing,
            Iterable<StorageProperty> addedProperties )
    {
        graphUpdates.getOrLoad( sourceNode ).createRelationship( internalRelationshipId, targetNode, type, outgoing, addedProperties );
    }

    @Override
    public void visitDeletedRelationship( long id, int type, long startNode, long endNode )
    {
        long internalRelationshipId = internalRelationshipIdFromRelationshipId( id );
        graphUpdates.getOrLoad( startNode ).deleteRelationship( internalRelationshipId, type, endNode, true );
        if ( startNode != endNode )
        {
            graphUpdates.getOrLoad( endNode ).deleteRelationship( internalRelationshipId, type, startNode, false );
        }
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
    {
        graphUpdates.getOrLoad( id ).updateNodeProperties( added, changed, removed );
    }

    @Override
    public void visitRelPropertyChanges( long relationshipId, int type, long startNode, long endNode,
            Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
    {
        long internalRelationshipId = internalRelationshipIdFromRelationshipId( relationshipId );
        graphUpdates.getOrLoad( startNode ).updateRelationshipProperties( internalRelationshipId, type, endNode, true,
                added, changed, removed );
        if ( startNode != endNode )
        {
            graphUpdates.getOrLoad( endNode ).updateRelationshipProperties( internalRelationshipId, type, startNode, false, added, changed, removed );
        }
    }

    @Override
    public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
    {
        graphUpdates.getOrLoad( id ).updateLabels( added, removed );
    }

    @Override
    public void visitAddedIndex( IndexDescriptor element )
    {
        commands.add( new FrekiCommand.Schema( element, Mode.CREATE ) );
    }

    @Override
    public void visitRemovedIndex( IndexDescriptor element )
    {
        commands.add( new FrekiCommand.Schema( element, Mode.DELETE ) );
    }

    @Override
    public void visitAddedConstraint( ConstraintDescriptor constraint ) throws KernelException
    {
        constraint = constraint.withId( stores.schemaStore.nextSchemaRuleId( cursorTracer ) );
        commands.add( new FrekiCommand.Schema( constraint, Mode.CREATE ) );
        switch ( constraint.type() )
        {
        case UNIQUE:
            // This also means updating the index to have this constraint as owner
            UniquenessConstraintDescriptor uniquenessConstraintDescriptor = constraint.asUniquenessConstraint();
            IndexDescriptor index = (IndexDescriptor) stores.schemaStore.loadRule( uniquenessConstraintDescriptor.ownedIndexId(), cursorTracer );
            commands.add( new FrekiCommand.Schema( index.withOwningConstraintId( constraint.getId() ), Mode.UPDATE ) );
            commands.add( new FrekiCommand.Schema(
                    constraintSemantics.createUniquenessConstraintRule( constraint.getId(), uniquenessConstraintDescriptor, index.getId() ), Mode.UPDATE ) );
            break;
        case UNIQUE_EXISTS:
            IndexDescriptor indexRule = (IndexDescriptor) stores.schemaStore.loadRule( constraint.asNodeKeyConstraint().ownedIndexId(), cursorTracer );
            commands.add( new FrekiCommand.Schema(
                    constraintSemantics.createNodeKeyConstraintRule( constraint.getId(), constraint.asNodeKeyConstraint(), indexRule.getId() ), Mode.UPDATE ) );
            break;
        case EXISTS:
            commands.add( new FrekiCommand.Schema( constraintSemantics.createExistenceConstraint( constraint.getId(), constraint ), Mode.CREATE ) );
            break;
        default:
            throw new UnsupportedOperationException( "Unknown constraint type " + constraint.type() );
        }
    }

    @Override
    public void visitRemovedConstraint( ConstraintDescriptor constraint ) throws KernelException
    {
        constraint = stores.schemaStore.loadRule( constraint, cursorTracer );
        commands.add( new FrekiCommand.Schema( constraint, Mode.DELETE ) );
        if ( constraint.type() == ConstraintType.UNIQUE )
        {
            // Remove the index for the constraint as well
            Iterator<IndexDescriptor> indexes = stores.schemaCache.indexesForSchema( constraint.schema() );
            for ( IndexDescriptor index : loop( indexes ) )
            {
                OptionalLong owningConstraintId = index.getOwningConstraintId();
                if ( owningConstraintId.isPresent() && owningConstraintId.getAsLong() == constraint.getId() )
                {
                    visitRemovedIndex( index );
                }
                // Note that we _could_ also go through all the matching indexes that have isUnique == true and no owning constraint id, and remove those
                // as well. These might be orphaned indexes from failed constraint creations. However, since we want to allow multiple indexes and
                // constraints on the same schema, they could also be constraint indexes that are currently populating for other constraints, and if that's
                // the case, then we cannot remove them, since that would ruin the constraint they are being built for.
            }
        }
    }

    @Override
    public void visitCreatedLabelToken( long id, String name, boolean internal )
    {
        commands.add( new FrekiCommand.LabelToken( new NamedToken( name, toIntExact( id ), internal ) ) );
    }

    @Override
    public void visitCreatedPropertyKeyToken( long id, String name, boolean internal )
    {
        commands.add( new FrekiCommand.PropertyKeyToken( new NamedToken( name, toIntExact( id ), internal ) ) );
    }

    @Override
    public void visitCreatedRelationshipTypeToken( long id, String name, boolean internal )
    {
        commands.add( new FrekiCommand.RelationshipTypeToken( new NamedToken( name, toIntExact( id ), internal ) ) );
    }

    @Override
    public void close() throws KernelException
    {
        graphUpdates.extractUpdates( commands::add );
    }
}
