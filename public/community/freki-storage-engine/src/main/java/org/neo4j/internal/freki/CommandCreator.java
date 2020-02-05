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
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
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
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.isDenseFromForwardPointer;
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
        mutations.put( id, createNew( stores.mainStore, id ) );
    }

    @Override
    public void visitDeletedNode( long id )
    {
        getOrLoad( id ).markAsUnused();
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
        Relationship relationship = sourceMutation.current.createRelationship( sourceNodeRelationship, targetNode, type );
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
            mutation.current.setNodeProperty( property.propertyKeyId(), property.value() );
        }
        removed.forEach( propertyKey -> mutation.current.removeNodeProperty( propertyKey ) );
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
    {
    }

    @Override
    public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
    {
        Mutation mutation = getOrLoad( id );
        mutation.small.addLabels( added );
        mutation.small.removeLabels( removed );
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
            RecordAndData abandonedLargeRecord = null;
            while ( true )
            {
                if ( mutation.current != mutation.small )
                {
                    // The record have grown into a larger record, although still prepare the small one due to potential changes
                    // TODO we should track whether or not we need to do this preparation actually
                    // TODO for the time being we expect this to work because there should only be labels in it, 60 or so should fit
                    mutation.small.prepareForCommandExtraction();
                }

                try
                {
                    mutation.current.prepareForCommandExtraction();
                    break;
                }
                catch ( BufferOverflowException | ArrayIndexOutOfBoundsException e )
                {
                    // TODO a silly thing to do, but catching this here makes this temporarily very simple to detect and grow into a larger record
                    RecordAndData largerRecord = mutation.current.growAndRelocate();
                    largerRecord.markInUse( true );
                    largerRecord.markCreated();
                    if ( mutation.large != null && !mutation.large.isCreated() && abandonedLargeRecord == null )
                    {
                        // Keep this abandoned large record around because we have to remove it below
                        abandonedLargeRecord = mutation.large;
                        abandonedLargeRecord.markInUse( false );
                    }
                    mutation.large = mutation.current = largerRecord;
                    mutation.small.prepareForCommandExtraction();
                }
            }

            mutation.small.createCommands( commands );
            if ( mutation.large != null )
            {
                mutation.large.createCommands( commands );
            }
            if ( abandonedLargeRecord != null )
            {
                abandonedLargeRecord.createCommands( commands );
            }
        } );
    }

    private Mutation createNew( SimpleStore store, long id )
    {
        Mutation mutation = new Mutation();
        mutation.small = new SparseRecordAndData( stores, store, id, null );
        mutation.small.markInUse( true );
        mutation.small.markCreated();
        mutation.current = mutation.small;
        return mutation;
    }

    private Mutation getOrLoad( long id )
    {
        Mutation mutation = mutations.get( id );
        if ( mutation == null )
        {
            mutation = new Mutation();
            mutation.small = new SparseRecordAndData( stores, stores.mainStore, id, null );
            mutation.small.loadExistingData();
            mutation.current = mutation.small;
            long forwardPointer = mutation.small.getForwardPointer();
            if ( forwardPointer != NULL )
            {
                // At this point it can still be either a larger record or a dense representation (GBPTree)
                boolean isDense = isDenseFromForwardPointer( forwardPointer );
                if ( !isDense )
                {
                    int fwSizeExp = sizeExponentialFromForwardPointer( forwardPointer );
                    long fwId = idFromForwardPointer( forwardPointer );
                    SimpleStore largeStore = stores.mainStore( fwSizeExp );
                    mutation.large = new SparseRecordAndData( stores, largeStore, fwId, mutation.small );
                    mutation.large.loadExistingData();
                    mutation.current = mutation.large;
                }
                else
                {
                    throw new UnsupportedOperationException( "Load dense not implemented yet" );
                }
            }
            mutations.put( id, mutation );
        }
        return mutation;
    }

    private static class Mutation
    {
        private SparseRecordAndData small;
        private RecordAndData large;
        // current points to either or
        private RecordAndData current;

        void markAsUnused()
        {
            small.markInUse( false );
            current.markInUse( false );
        }
    }

    private static abstract class RecordAndData
    {
        abstract void markCreated();

        abstract boolean isCreated();

        abstract void markInUse( boolean inUse );

        abstract void loadExistingData();

        abstract Relationship createRelationship( Relationship sourceNodeRelationship, long targetNode, int type );

        abstract void setNodeProperty( int propertyKeyId, Value value );

        abstract void removeNodeProperty( int propertyKeyId );

        abstract void addLabels( LongSet added );

        abstract void removeLabels( LongSet removed );

        abstract void prepareForCommandExtraction();

        abstract RecordAndData growAndRelocate();

        abstract long asForwardPointer();

        abstract void createCommands( Collection<StorageCommand> commands );
    }

    private static class SparseRecordAndData extends RecordAndData
    {
        private final MainStores stores;
        private final SimpleStore store;
        private final long id;
        private final SparseRecordAndData smallRecord;

        private boolean created;
        private Record before;
        private Record after;
        // data here is really accidental state, a deserialized objectified version of the data found in the byte[] of the "after" record
        private MutableNodeRecordData data;

        SparseRecordAndData( MainStores stores, SimpleStore store, long id, SparseRecordAndData smallRecord )
        {
            this.stores = stores;
            this.store = store;
            this.id = id;
            this.smallRecord = smallRecord != null ? smallRecord : this;
            before = new Record( store.recordSizeExponential(), id );
            after = new Record( store.recordSizeExponential(), id );
            data = new MutableNodeRecordData( id );
        }

        @Override
        void markCreated()
        {
            created = true;
        }

        @Override
        public boolean isCreated()
        {
            return created;
        }

        @Override
        void markInUse( boolean inUse )
        {
            after.setFlag( FLAG_IN_USE, inUse );
        }

        @Override
        void loadExistingData()
        {
            try ( PageCursor cursor = store.openReadCursor() )
            {
                store.read( cursor, before, id );
            }
            data.deserialize( before.dataForReading() );
            after.copyContentsFrom( before );
        }

        @Override
        Relationship createRelationship( Relationship sourceNodeRelationship, long targetNode, int type )
        {
            return data.createRelationship( sourceNodeRelationship, targetNode, type );
        }

        @Override
        void setNodeProperty( int propertyKeyId, Value value )
        {
            data.setNodeProperty( propertyKeyId, value );
        }

        @Override
        void removeNodeProperty( int propertyKeyId )
        {
            data.removeNodeProperty( propertyKeyId );
        }

        @Override
        void addLabels( LongSet added )
        {
            added.forEach( label -> data.labels.add( toIntExact( label ) ) );
        }

        @Override
        void removeLabels( LongSet removed )
        {
            removed.forEach( label -> data.labels.remove( toIntExact( label ) ) );
        }

        long getForwardPointer()
        {
            return data.getForwardPointer();
        }

        @Override
        void prepareForCommandExtraction()
        {
            data.serialize( after.dataForWriting() );
        }

        @Override
        RecordAndData growAndRelocate()
        {
            SimpleStore largerStore = stores.nextLargerMainStore( after.sizeExp() );
            RecordAndData result;
            if ( largerStore != null )
            {
                SparseRecordAndData largerRecord = new SparseRecordAndData( stores, largerStore, largerStore.nextId( PageCursorTracer.NULL ), smallRecord );
                data.movePropertiesAndRelationshipsTo( largerRecord.data );
                result = largerRecord;
            }
            else
            {
                // Time to move over to GBPTree-style data
                DenseRecordAndData denseRecord = new DenseRecordAndData( stores.denseStore, id, smallRecord );
                denseRecord.movePropertiesAndRelationshipsFrom( data );
                result = denseRecord;
            }
            smallRecord.data.setForwardPointer( result.asForwardPointer() );
            return result;
        }

        @Override
        long asForwardPointer()
        {
            return forwardPointer( store.recordSizeExponential(), false, id );
        }

        @Override
        void createCommands( Collection<StorageCommand> commands )
        {
            commands.add( new FrekiCommand.SparseNode( before, after ) );
        }
    }

    private static class DenseRecordAndData extends RecordAndData
    {
        // meta
        private final DenseStore store;
        private final long id;
        private final SparseRecordAndData smallRecord;
        private boolean created;
        private boolean inUse;

        // changes
        // TODO it feels like we've simply moving tx-state data from one form to another and that's probably true and can probably be improved on later
        private MutableIntObjectMap<MutableNodeRecordData.Relationships> createdRelationships = IntObjectMaps.mutable.empty();
        private MutableIntObjectMap<MutableNodeRecordData.Relationships> deletedRelationships = IntObjectMaps.mutable.empty();
        private MutableIntObjectMap<Value> addedProperties = IntObjectMaps.mutable.empty();
        private MutableIntSet removedProperties = IntSets.mutable.empty();

        DenseRecordAndData( DenseStore store, long id, SparseRecordAndData smallRecord )
        {
            this.store = store;
            this.id = id;
            this.smallRecord = smallRecord;
        }

        @Override
        void markCreated()
        {
            created = true;
        }

        @Override
        boolean isCreated()
        {
            return created;
        }

        @Override
        void markInUse( boolean inUse )
        {
            this.inUse = inUse;
        }

        @Override
        void loadExistingData()
        {
            // Thoughts: we shouldn't really load anything here since we don't need the existing data in order to make changes
        }

        @Override
        Relationship createRelationship( Relationship sourceNodeRelationship, long targetNode, int type )
        {
            long internalRelationshipId = sourceNodeRelationship == null
                    ? smallRecord.data.nextInternalRelationshipId()
                    : sourceNodeRelationship.internalId;
            boolean outgoing = sourceNodeRelationship == null;
            return createdRelationships.getIfAbsentPutWithKey( type, MutableNodeRecordData.Relationships::new )
                    .add( internalRelationshipId, id, targetNode, type, outgoing );
        }

        @Override
        void setNodeProperty( int propertyKeyId, Value value )
        {
            removedProperties.remove( propertyKeyId );
            addedProperties.put( propertyKeyId, value );
        }

        @Override
        void removeNodeProperty( int propertyKeyId )
        {
            addedProperties.remove( propertyKeyId );
            removedProperties.add( propertyKeyId );
        }

        @Override
        void addLabels( LongSet added )
        {
            throw new UnsupportedOperationException( "Should not happen, right?" );
        }

        @Override
        void removeLabels( LongSet removed )
        {
            throw new UnsupportedOperationException( "Should not happen, right?" );
        }

        @Override
        void prepareForCommandExtraction()
        {
            // Thoughts: no special preparation should be required here either
        }

        @Override
        RecordAndData growAndRelocate()
        {
            throw new UnsupportedOperationException( "There should be no need to grow into something bigger" );
        }

        @Override
        long asForwardPointer()
        {
            // Thoughts: the nodeId comes into play when we have node-centric trees, where the root id can be the nodeId here
            return forwardPointer( 0, true, 0 );
        }

        @Override
        void createCommands( Collection<StorageCommand> commands )
        {
            // Thoughts: take all the logical changes and add to logical commands, either separate for each change or one big command containing all
            commands.add( new FrekiCommand.DenseNode( id, inUse, addedProperties, removedProperties, createdRelationships, deletedRelationships ) );
        }

        void movePropertiesAndRelationshipsFrom( MutableNodeRecordData data )
        {
            addedProperties.putAll( data.properties );
            createdRelationships.putAll( data.relationships );
            data.clearPropertiesAndRelationships();
        }
    }
}
