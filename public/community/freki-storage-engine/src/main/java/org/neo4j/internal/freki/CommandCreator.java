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
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.api.NamedToken;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;

class CommandCreator implements TxStateVisitor
{
    private final Collection<StorageCommand> commands;
    private final Stores stores;
    private final MutableLongObjectMap<FrekiCommand> build = LongObjectMaps.mutable.empty();

    public CommandCreator( Collection<StorageCommand> commands, Stores stores )
    {
        this.commands = commands;
        this.stores = stores;
    }

    @Override
    public void visitCreatedNode( long id )
    {
        Record after = new Record( 1, id );
        after.setFlag( FLAG_IN_USE );
        after.node = new MutableNodeRecordData();
        Record before = new Record( 1, id );
        before.node = new MutableNodeRecordData();
        build.put( id, new FrekiCommand.Node( before, after ) );
    }

    @Override
    public void visitDeletedNode( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode ) throws ConstraintValidationException
    {
        //TODO use the ID for something useful!
        createRelationship( id, type, startNode, endNode, true );
        if ( startNode != endNode )
        {
            createRelationship( id, type, endNode, startNode, false );
        }
    }

    private void createRelationship( long id, int type, long firstNode, long secondNode, boolean outgoing )
    {
        FrekiCommand.Node startCommand = (FrekiCommand.Node) build.get( firstNode );
        startCommand.after().node.relationships.put( id, new MutableNodeRecordData.Relationship( secondNode, type, outgoing ) );
    }

    @Override
    public void visitDeletedRelationship( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
            throws ConstraintValidationException
    {
        FrekiCommand.Node command = (FrekiCommand.Node) build.get( id );
        assertExists( command );
        while ( added.hasNext() )
        {
            StorageProperty property = added.next();
            command.after().node.properties.put( property.propertyKeyId(), new MutableNodeRecordData.Property( property.propertyKeyId(), property.value() ) );
        }
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
            throws ConstraintValidationException
    {
//        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitNodeLabelChanges( long id, LongSet added, LongSet removed ) throws ConstraintValidationException
    {
        FrekiCommand.Node command = (FrekiCommand.Node) build.get( id );
        assertExists( command );
        if ( !removed.isEmpty() )
        {
            throw new UnsupportedOperationException( "Not implemented yet" );
        }

        // Add the new labels into the record
        command.after().node.labels = toSortedIntArray( added );
    }

    private void assertExists( FrekiCommand.Node command )
    {
        if ( command == null )
        {
            // Changed, not created
            throw new UnsupportedOperationException( "Not implemented yet" );
        }
    }

    private static int[] toSortedIntArray( LongSet set )
    {
        int[] result = new int[set.size()];
        LongIterator iterator = set.longIterator();
        for ( int i = 0; iterator.hasNext(); i++ )
        {
            result[i] = toIntExact( iterator.next() );
        }
        Arrays.sort( result );
        return result;
    }

    @Override
    public void visitAddedIndex( IndexDescriptor element ) throws KernelException
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitRemovedIndex( IndexDescriptor element )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitAddedConstraint( ConstraintDescriptor element ) throws KernelException
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitRemovedConstraint( ConstraintDescriptor element )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
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
    public void close()
    {
        build.each( commands::add );
    }
}
