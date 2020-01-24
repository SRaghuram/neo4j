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

import java.nio.BufferOverflowException;
import java.util.Collection;

import org.neo4j.internal.freki.MutableNodeRecordData.Relationship;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.api.NamedToken;
import org.neo4j.util.Preconditions;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromForwardPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;

class CommandCreator implements TxStateVisitor
{
    private final Collection<StorageCommand> commands;
    private final Stores stores;
    private final MutableLongObjectMap<Mutation> mutations = LongObjectMaps.mutable.empty();

    public CommandCreator( Collection<StorageCommand> commands, Stores stores )
    {
        this.commands = commands;
        this.stores = stores;
    }

    @Override
    public void visitCreatedNode( long id )
    {
        mutations.put( id, createNew( id, stores.mainStore ) );
    }

    @Override
    public void visitDeletedNode( long id )
    {
        getOrLoad( id ).current.after.setFlag( FLAG_IN_USE, false );
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode, Iterable<StorageProperty> addedProperties )
    {
        // TODO we cannot use the provided id since it comes from kernel which generated a relationship ID from an IdGenerator.
        //      The actual ID that this relationship will get is something else, something based on the node ID and other data found in the node itself.
        //      This is going to require some changes in the kernel, or at least some restrictions on how the Core API can expect to make use of
        //      relationship IDs that gets created inside a transaction (the ID of the relationship read from the store later on will be different.

        Relationship relationshipAtStartNode = createRelationship( null, type, startNode, endNode, addedProperties );
        if ( startNode != endNode )
        {
            createRelationship( relationshipAtStartNode, type, endNode, startNode, addedProperties );
        }
    }

    private Relationship createRelationship( Relationship sourceNodeRelationship, int type, long sourceNode, long targetNode,
            Iterable<StorageProperty> addedProperties )
    {
        Mutation sourceMutation = getOrLoad( sourceNode );
        Relationship relationship = sourceMutation.current.data.createRelationship( sourceNodeRelationship, targetNode, type );
        for ( StorageProperty property : addedProperties )
        {
            relationship.addProperty( property.propertyKeyId(), property.value() );
        }
        return relationship;
    }

    @Override
    public void visitDeletedRelationship( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
    {
        Mutation mutation = getOrLoad( id );
        for ( StorageProperty property : added )
        {
            mutation.current.data.setNodeProperty( property.propertyKeyId(), property.value() );
        }
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
    {
    }

    @Override
    public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
    {
        Mutation mutation = getOrLoad( id );

        // Add the new labels into the record
        LongIterator addedIterator = added.longIterator();
        while ( addedIterator.hasNext() )
        {
            mutation.current.data.labels.add( toIntExact( addedIterator.next() ) );
        }

        // Remove the removed labels
        LongIterator removedIterator = removed.longIterator();
        while ( removedIterator.hasNext() )
        {
            mutation.current.data.labels.remove( toIntExact( removedIterator.next() ) );
        }
    }

    @Override
    public void visitAddedIndex( IndexDescriptor element )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitRemovedIndex( IndexDescriptor element )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitAddedConstraint( ConstraintDescriptor element )
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
        mutations.each( mutation ->
        {
            try
            {
                // first try the current record size
                mutation.current.data.serialize( mutation.current.after.dataForWriting() );
            }
            catch ( BufferOverflowException | ArrayIndexOutOfBoundsException e )
            {
                // TODO a silly thing to do, but catching this here makes this temporarily very simple to detect and grow into a larger record
                // didn't fit, try a larger record, if it hasn't got one already (let's keep it simple for now)
                Preconditions.checkState( mutation.large == null, "Really expected the large record to not exist at this point of overflow" );
                SimpleStore largeStore = stores.nextLargerMainStore( mutation.small.after.sizeExp() );
                long largeId = largeStore.nextId( PageCursorTracer.NULL );
                mutation.large = new RecordAndData();
                mutation.large.before = new Record( largeStore.recordSizeExponential(), largeId );
                mutation.large.after = new Record( largeStore.recordSizeExponential(), largeId );
                mutation.large.after.setFlag( FLAG_IN_USE, true );
                mutation.large.data = new MutableNodeRecordData( largeId );
                mutation.current = mutation.large;

                mutation.small.data.moveDataTo( mutation.large.data );
                mutation.small.data.setForwardPointer( forwardPointer( largeStore.recordSizeExponential(), largeId ) );
                mutation.small.data.serialize( mutation.small.after.dataForWriting() );

                // TODO if we get overflow here we'd better move to GBPTree, but that's the next step
                mutation.large.data.serialize( mutation.large.after.dataForWriting() );
            }

            commands.add( new FrekiCommand.Node( mutation.small.before, mutation.small.after ) );
            if ( mutation.large != null )
            {
                commands.add( new FrekiCommand.Node( mutation.large.before, mutation.large.after ) );
            }
        } );
    }

    private Mutation createNew( long id, SimpleStore store )
    {
        Mutation mutation = new Mutation();
        mutation.small = new RecordAndData();
        mutation.small.before = new Record( store.recordSizeExponential(), id );
        mutation.small.after = new Record( store.recordSizeExponential(), id );
        mutation.small.after.setFlag( FLAG_IN_USE, true );
        mutation.small.data = new MutableNodeRecordData( id );
        mutation.current = mutation.small;
        return mutation;
    }

    private Mutation getOrLoad( long id )
    {
        Mutation mutation = mutations.get( id );
        if ( mutation == null )
        {
            mutation = new Mutation();
            mutation.small = new RecordAndData();
            mutation.small.before = new Record( stores.mainStore.recordSizeExponential() );
            try ( PageCursor cursor = stores.mainStore.openReadCursor() )
            {
                stores.mainStore.read( cursor, mutation.small.before, id );
            }
            mutation.small.data = new MutableNodeRecordData( id );
            mutation.small.data.deserialize( mutation.small.before.dataForReading() );
            mutation.small.after = new Record( stores.mainStore.recordSizeExponential() );
            mutation.small.after.copyContentsFrom( mutation.small.before );
            mutation.current = mutation.small;

            long forwardPointer = mutation.small.data.getForwardPointer();
            if ( forwardPointer != NULL )
            {
                int fwSizeExp = sizeExponentialFromForwardPointer( forwardPointer );
                long fwId = idFromForwardPointer( forwardPointer );
                SimpleStore largeStore = stores.mainStore( fwSizeExp );
                mutation.large = new RecordAndData();
                mutation.large.before = new Record( fwSizeExp );
                try ( PageCursor cursor = largeStore.openReadCursor() )
                {
                    largeStore.read( cursor, mutation.large.before, fwId );
                }
                mutation.large.data = new MutableNodeRecordData( fwId );
                mutation.large.data.deserialize( mutation.large.before.dataForReading() );
                mutation.large.after = new Record( fwSizeExp, fwId );
                mutation.large.after.copyContentsFrom( mutation.large.before );
                mutation.current = mutation.large;
            }
            mutations.put( id, mutation );
        }
        return mutation;
    }

    private static class Mutation
    {
        private RecordAndData small;
        private RecordAndData large;
        // current points to either or
        private RecordAndData current;
    }

    private static class RecordAndData
    {
        private Record before;
        private Record after;
        // data here is really accidental state, a deserialized objectified version of the data found in after
        private MutableNodeRecordData data;
    }
}
