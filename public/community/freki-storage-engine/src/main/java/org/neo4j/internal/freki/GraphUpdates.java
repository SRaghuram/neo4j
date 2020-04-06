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
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationships;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeRecordData.FLAG_DEGREES;
import static org.neo4j.internal.freki.MutableNodeRecordData.FLAG_PROPERTIES;
import static org.neo4j.internal.freki.MutableNodeRecordData.FLAG_RELATIONSHIPS;
import static org.neo4j.internal.freki.MutableNodeRecordData.buildRecordPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;

/**
 * Contains all logic about making graph data changes to a Freki store, everything from loading and modifying data to serializing
 * and overflowing into larger records or dense store.
 */
class GraphUpdates
{
    private final Collection<StorageCommand> bigValueCommands = new ArrayList<>();
    private final Consumer<StorageCommand> bigValueCommandConsumer = bigValueCommands::add;
    private final MutableLongObjectMap<NodeUpdates> mutations = LongObjectMaps.mutable.empty();
    private final MainStores stores;
    private final PageCursorTracer cursorTracer;

    GraphUpdates( MainStores stores, PageCursorTracer cursorTracer )
    {
        this.stores = stores;
        this.cursorTracer = cursorTracer;
    }

    NodeUpdates getOrLoad( long nodeId )
    {
        return mutations.getIfAbsentPut( nodeId, () ->
        {
            NodeUpdates updates = new NodeUpdates( nodeId, stores, bigValueCommandConsumer, cursorTracer );
            updates.load();
            return updates;
        } );
    }

    void create( long nodeId )
    {
        mutations.put( nodeId, new NodeUpdates( nodeId, stores, bigValueCommandConsumer, cursorTracer ) );
    }

    void extractUpdates( Consumer<StorageCommand> commands ) throws ConstraintViolationTransactionFailureException
    {
        List<StorageCommand> otherCommands = new ArrayList<>();
        ByteBuffer smallBuffer = ByteBuffer.wrap( new byte[stores.mainStore.recordDataSize()] );
        ByteBuffer maxBuffer = ByteBuffer.wrap( new byte[stores.largestMainStore().recordDataSize()] );
        for ( NodeUpdates mutation : mutations )
        {
            mutation.serialize( smallBuffer, maxBuffer, otherCommands::add );
        }
        bigValueCommands.forEach( commands );
        otherCommands.forEach( commands );
    }

    private abstract static class NodeDataModifier
    {
        abstract void updateLabels( LongSet added, LongSet removed );

        abstract void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed );

        abstract void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties );

        abstract void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing );

        abstract void delete();
    }

    static class NodeUpdates extends NodeDataModifier
    {
        private final long nodeId;
        private final MainStores stores;
        private final Consumer<StorageCommand> bigValueCommandConsumer;
        private final PageCursorTracer cursorTracer;

        // the before-state
        private Record x1Before;
        private Record xLBefore;

        // the after-state
        private SparseRecordAndData sparse;
        private DenseRecordAndData dense;
        private boolean deleted;

        NodeUpdates( long nodeId, MainStores stores, Consumer<StorageCommand> bigValueCommandConsumer, PageCursorTracer cursorTracer )
        {
            this.nodeId = nodeId;
            this.stores = stores;
            this.bigValueCommandConsumer = bigValueCommandConsumer;
            this.cursorTracer = cursorTracer;
            this.sparse = new SparseRecordAndData( nodeId ); // this will always exist
        }

        long nodeId()
        {
            return nodeId;
        }

        void load()
        {
            Record x1 = readRecord( stores, 0, nodeId, cursorTracer );
            if ( x1 == null )
            {
                throw new IllegalStateException( "Node[" + nodeId + "] should have existed" );
            }

            sparse.data.deserialize( x1.data(), stores.bigPropertyValueStore, cursorTracer );
            x1Before = x1;

            long recordPointer = sparse.data.getRecordPointer();
            if ( recordPointer != NULL )
            {
                Record xL = readRecord( stores, sizeExponentialFromRecordPointer( recordPointer ), idFromRecordPointer( recordPointer ), cursorTracer );
                MutableNodeRecordData largeData = new MutableNodeRecordData( nodeId );
                largeData.deserialize( xL.data(), stores.bigPropertyValueStore, cursorTracer );
                sparse.data.copyDataFrom( FLAG_PROPERTIES | FLAG_DEGREES | FLAG_RELATIONSHIPS, largeData );
                xLBefore = xL;
            }
            if ( sparse.data.isDense() )
            {
                dense = new DenseRecordAndData( sparse, stores.denseStore, stores.bigPropertyValueStore, bigValueCommandConsumer, cursorTracer );
            }
        }

        private NodeDataModifier forLabels()
        {
            return sparse;
        }

        private NodeDataModifier forProperties()
        {
            return sparse;
        }

        private NodeDataModifier forRelationships()
        {
            return dense != null ? dense : sparse;
        }

        @Override
        public void updateLabels( LongSet added, LongSet removed )
        {
            forLabels().updateLabels( added, removed );
        }

        @Override
        public void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            forProperties().updateNodeProperties( added, changed, removed );
        }

        @Override
        public void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties )
        {
            forRelationships().createRelationship( internalId, targetNode, type, outgoing, properties );
        }

        @Override
        public void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            forRelationships().deleteRelationship( internalId, type, otherNode, outgoing );
        }

        @Override
        public void delete()
        {
            deleted = true;
            sparse.delete();
            if ( dense != null )
            {
                dense.delete();
            }
        }

        private void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            sparse.prepareForCommandExtraction();
            if ( dense != null )
            {
                dense.prepareForCommandExtraction();
            }
        }

        void serialize( ByteBuffer smallBuffer, ByteBuffer maxBuffer, Consumer<StorageCommand> otherCommands )
                throws ConstraintViolationTransactionFailureException
        {
            prepareForCommandExtraction();

            if ( deleted )
            {
                if ( x1Before != null )
                {
                    otherCommands.accept( new FrekiCommand.SparseNode( nodeId, x1Before, deletedVersionOf( x1Before ) ) );
                }
                if ( xLBefore != null )
                {
                    otherCommands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, deletedVersionOf( xLBefore ) ) );
                }
                if ( dense != null )
                {
                    dense.createCommands( otherCommands );
                }
                return;
            }

            // === ATTEMPT serialize everything into x1 ===
            // The reason we try x1 first is that there'll be overhead in the form of forward pointer and potentially next-relationship-id inside x1
            // if points to a larger record.
            long prevRecordPointer = sparse.data.getRecordPointer();
            sparse.data.setRecordPointer( NULL );
            if ( trySerialize( sparse.data, smallBuffer.clear() ) )
            {
                x1Command( smallBuffer, otherCommands );
                if ( xLBefore != null )
                {
                    otherCommands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, deletedVersionOf( xLBefore ) ) );
                }
            }
            else
            {
                sparse.data.setRecordPointer( prevRecordPointer );
                // Then try various constellations of larger records (make sure sparse.data is left with correct data to be serialized as part of this call)
                moveDataToAndSerializeLargerRecords( maxBuffer, otherCommands );

                // After that has been done serialize x1 because it's likely pointing to one or more larger records
                if ( !trySerialize( sparse.data, smallBuffer ) )
                {
                    throw new UnsupportedOperationException( "Couldn't serialize x1 after some data moved from it" );
                }
                x1Command( smallBuffer, otherCommands );
            }
            if ( dense != null )
            {
                dense.createCommands( otherCommands );
            }
        }

        private void moveDataToAndSerializeLargerRecords( ByteBuffer maxBuffer, Consumer<StorageCommand> otherCommands )
        {
            // Can we fit labels+properties in x1?
            //   - yes: good, just place relationships in xL or DENSE
            // Can we fit properties+relationships in xL?
            //   - yes: good
            // Can we fit properties in xL?
            //   - yes: good, just place relationships in DENSE

            // TODO === ATTEMPT move relationships to a larger x ===

            // === ATTEMPT move properties, degrees and relationships to a larger x ===
            {
                MutableNodeRecordData largeData = new MutableNodeRecordData( nodeId );
                largeData.copyDataFrom( FLAG_PROPERTIES | FLAG_DEGREES | FLAG_RELATIONSHIPS, sparse.data );
                largeData.setRecordPointer( buildRecordPointer( 0, nodeId ) );
                if ( trySerialize( largeData, maxBuffer.clear() ) )
                {
                    // We were able to fit properties and relationships into this larger store.
                    SimpleStore store = stores.storeSuitableForRecordSize( maxBuffer.limit(), 1 );
                    xLargeCommands( maxBuffer, store, otherCommands, d -> d.clearData( FLAG_PROPERTIES | FLAG_DEGREES | FLAG_RELATIONSHIPS ) );
                    return;
                }
            }

            // === ATTEMPT move properties and degrees to a larger x (and relationships to dense store) ===
            {
                moveDataToDense();
                MutableNodeRecordData largeData = new MutableNodeRecordData( nodeId );
                largeData.copyDataFrom( FLAG_PROPERTIES | FLAG_DEGREES, sparse.data );
                largeData.setRecordPointer( buildRecordPointer( 0, nodeId ) );
                sparse.data.setDense( true );
                largeData.setDense( true );
                if ( trySerialize( largeData, maxBuffer.clear() ) )
                {
                    // We were able to fit properties and relationships into this larger store. The reason we try x1 first is that there'll be overhead
                    // in the form of forward pointer and potentially next-relationship-id inside x1 if points to a larger record.
                    SimpleStore store = stores.storeSuitableForRecordSize( maxBuffer.limit(), 1 );
                    xLargeCommands( maxBuffer, store, otherCommands, d -> d.clearData( FLAG_PROPERTIES | FLAG_DEGREES ) );
                    return;
                }
            }

            throw new UnsupportedOperationException( "Properties must fit in an x record a.t.m." );

//            // === Move properties and relationships to dense
//            {
//                if ( xLBefore != null )
//                {
//                    otherCommands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, deletedVersionOf( xLBefore ) ) );
//                }
//                moveDataToDense( FLAG_PROPERTIES | FLAG_RELATIONSHIPS );
//                // TODO update the x1 fw-pointer to also say dense
//            }
        }

        private Record deletedVersionOf( Record record )
        {
            Record deletedRecord = new Record( record.sizeExp(), record.id );
            deletedRecord.setFlag( FLAG_IN_USE, false );
            return deletedRecord;
        }

        private void moveDataToDense()
        {
            if ( dense == null )
            {
                dense = new DenseRecordAndData( sparse, stores.denseStore, stores.bigPropertyValueStore, bigValueCommandConsumer, cursorTracer );
            }
            dense.moveDataFrom( sparse.data );
        }

        private boolean trySerialize( MutableNodeRecordData data, ByteBuffer buffer )
        {
            try
            {
                data.serialize( buffer, stores.bigPropertyValueStore, bigValueCommandConsumer );
                return true;
            }
            catch ( BufferOverflowException | ArrayIndexOutOfBoundsException e )
            {
                return false;
            }
        }

        private void xLargeCommands( ByteBuffer maxBuffer, SimpleStore store, Consumer<StorageCommand> commands,
                Consumer<MutableNodeRecordData> smallDataModifier )
        {
            Record after;
            int sizeExp = store.recordSizeExponential();
            if ( xLBefore != null && xLBefore.sizeExp() == sizeExp )
            {
                // There was a large record before and we're just modifying it
                commands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, after = recordForData( xLBefore.id, maxBuffer, sizeExp ) ) );
            }
            else if ( xLBefore != null && xLBefore.sizeExp() != sizeExp )
            {
                // There was a large record before, but this time it'll be of a different size, so a different one
                commands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, deletedVersionOf( xLBefore ) ) );
                long recordId = store.nextId( cursorTracer );
                commands.accept(
                        new FrekiCommand.SparseNode( nodeId, new Record( sizeExp, recordId ), after = recordForData( recordId, maxBuffer, sizeExp ) ) );
            }
            else
            {
                // There was no large record before at all
                long recordId = store.nextId( cursorTracer );
                commands.accept(
                        new FrekiCommand.SparseNode( nodeId, new Record( sizeExp, recordId ), after = recordForData( recordId, maxBuffer, sizeExp ) ) );
            }

            sparse.data.setRecordPointer( buildRecordPointer( after.sizeExp(), after.id ) );
            smallDataModifier.accept( sparse.data );
        }

        private void x1Command( ByteBuffer smallBuffer, Consumer<StorageCommand> commands )
        {
            Record before = x1Before != null ? x1Before : new Record( 0, nodeId );
            Record after = recordForData( nodeId, smallBuffer, 0 );
            commands.accept( new FrekiCommand.SparseNode( nodeId, before, after ) );
        }
    }

    private static class SparseRecordAndData extends NodeDataModifier
    {
        private MutableNodeRecordData data;
        private boolean deleted;

        SparseRecordAndData( long nodeId )
        {
            this.data = new MutableNodeRecordData( nodeId );
        }

        @Override
        public void updateLabels( LongSet added, LongSet removed )
        {
            added.forEach( label -> data.labels.add( toIntExact( label ) ) );
            removed.forEach( label -> data.labels.remove( toIntExact( label ) ) );
        }

        @Override
        public void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties )
        {
            MutableNodeRecordData.Relationship relationship = data.createRelationship( internalId, targetNode, type, outgoing );
            for ( StorageProperty property : properties )
            {
                relationship.addProperty( property.propertyKeyId(), property.value() );
            }
        }

        @Override
        public void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            added.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            changed.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            removed.forEach( p -> data.removeNodeProperty( p ) );
        }

        @Override
        public void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            data.deleteRelationship( internalId, type, otherNode, outgoing );
        }

        @Override
        public void delete()
        {
            this.deleted = true;
        }

        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            // Sanity-check so that, if this node has been deleted it cannot have any relationships left in it
            if ( deleted && data.hasRelationships() )
            {
                throw new DeletedNodeStillHasRelationships( data.nodeId );
            }
        }
    }

    private static class DenseRecordAndData extends NodeDataModifier
    {
        // meta
        private final SparseRecordAndData smallRecord;
        private final DenseRelationshipStore store;
        private final SimpleBigValueStore bigValueStore;
        private final Consumer<StorageCommand> bigValueConsumer;
        private final PageCursorTracer cursorTracer;
        private boolean deleted;

        // changes
        // TODO it feels like we've simply moving tx-state data from one form to another and that's probably true and can probably be improved on later
        private MutableIntObjectMap<DenseRelationships> relationshipUpdates = IntObjectMaps.mutable.empty();

        DenseRecordAndData( SparseRecordAndData smallRecord, DenseRelationshipStore store, SimpleBigValueStore bigValueStore,
                Consumer<StorageCommand> bigValueConsumer, PageCursorTracer cursorTracer )
        {
            this.smallRecord = smallRecord;
            this.store = store;
            this.bigValueStore = bigValueStore;
            this.bigValueConsumer = bigValueConsumer;
            this.cursorTracer = cursorTracer;
        }

        private long nodeId()
        {
            return smallRecord.data.nodeId;
        }

        @Override
        public void updateLabels( LongSet added, LongSet removed )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> addedProperties )
        {
            createRelationship( internalId, targetNode, type, outgoing, serializeAddedProperties( addedProperties, IntObjectMaps.mutable.empty() ) );
        }

        private void createRelationship( long internalId, long targetNode, int type, boolean outgoing, IntObjectMap<PropertyUpdate> properties )
        {
            // For sparse representation the high internal relationship ID counter is simply the highest of the existing relationships,
            // decided when loading the node. But for dense nodes we won't load all relationships and will therefore need to keep
            // this counter in an explicit field in the small record. This call keeps that counter updated.
            smallRecord.data.registerInternalRelationshipId( internalId );
            smallRecord.data.degrees.add( type, calculateDirection( targetNode, outgoing ), 1 );
            relationshipUpdatesForType( type ).create( new DenseRelationships.DenseRelationship( internalId, targetNode, outgoing, properties ) );
        }

        private RelationshipDirection calculateDirection( long targetNode, boolean outgoing )
        {
            return nodeId() == targetNode ? RelationshipDirection.LOOP : outgoing ? RelationshipDirection.OUTGOING : RelationshipDirection.INCOMING;
        }

        private DenseRelationships relationshipUpdatesForType( int type )
        {
            return relationshipUpdates.getIfAbsentPutWithKey( type, t -> new DenseRelationships( nodeId(), t ) );
        }

        @Override
        public void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            // TODO have some way of at least saying whether or not this relationship had properties, so that this loading can be skipped completely
            smallRecord.data.degrees.add( type, calculateDirection( otherNode, outgoing ), -1 );
            relationshipUpdatesForType( type ).delete( new DenseRelationships.DenseRelationship( internalId, otherNode, outgoing,
                    store.loadRelationshipPropertiesForRemoval( nodeId(), internalId, type, otherNode, outgoing, cursorTracer ) ) );
        }

        @Override
        public void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete()
        {
            deleted = true;
        }

        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            if ( deleted )
            {
                // This dense node has now been deleted, verify that all its relationships have also been removed in this transaction
                if ( !smallRecord.data.degrees.isEmpty() )
                {
                    throw new DeletedNodeStillHasRelationships( nodeId() );
                }
            }
        }

        void createCommands( Consumer<StorageCommand> commands )
        {
            commands.accept( new FrekiCommand.DenseNode( nodeId(), relationshipUpdates ) );
        }

        /**
         * Moving from a sparse --> dense will have all data look like "created", since the before-state is the before-state of the sparse record
         * that it comes from. This differs from changes to an existing dense node where all changes need to follow the added/changed/removed pattern.
         */
        void moveDataFrom( MutableNodeRecordData data )
        {
            // We're moving to the dense store, which from its POV all relationships will be created so therefore
            // start at 0 degrees and all creations will increment those degrees.
            data.relationships.forEachKeyValue( ( type, fromRelationships ) -> fromRelationships.relationships.forEach(
                    from -> createRelationship( from.internalId, from.otherNode, from.type, from.outgoing,
                            serializeAddedProperties( from.properties, IntObjectMaps.mutable.empty() ) ) ) );
            smallRecord.data.nextInternalRelationshipId = data.nextInternalRelationshipId;
            data.clearData( FLAG_RELATIONSHIPS );
        }

        private IntObjectMap<PropertyUpdate> serializeAddedProperties( IntObjectMap<Value> properties, MutableIntObjectMap<PropertyUpdate> target )
        {
            properties.forEachKeyValue( ( key, value ) -> target.put( key, add( key, serializeValue( bigValueStore, value, bigValueConsumer ) ) ) );
            return target;
        }

        private IntObjectMap<PropertyUpdate> serializeAddedProperties( Iterable<StorageProperty> properties, MutableIntObjectMap<PropertyUpdate> target )
        {
            properties.forEach( property -> target.put( property.propertyKeyId(),
                    add( property.propertyKeyId(), serializeValue( bigValueStore, property.value(), bigValueConsumer ) ) ) );
            return target;
        }
    }

    private static Record recordForData( long recordId, ByteBuffer buffer, int sizeExp )
    {
        Record after = new Record( sizeExp, recordId );
        after.setFlag( FLAG_IN_USE, true );
        ByteBuffer byteBuffer = after.data();
        byteBuffer.put( buffer.array(), 0, buffer.limit() );
        return after;
    }

    private static Record readRecord( MainStores stores, int sizeExp, long id, PageCursorTracer cursorTracer )
    {
        SimpleStore store = stores.mainStore( sizeExp );
        try ( PageCursor cursor = store.openReadCursor( cursorTracer ) )
        {
            if ( store.exists( cursor, id ) )
            {
                Record record = store.newRecord();
                store.read( cursor, record, id );
                return record;
            }
            return null;
        }
    }

    static ByteBuffer serializeValue( SimpleBigValueStore bigValueStore, Value value, Consumer<StorageCommand> bigValueCommandConsumer )
    {
        // TODO hand-wavy upper limit
        ByteBuffer buffer = ByteBuffer.wrap( new byte[256] );
        PropertyValueFormat format = new PropertyValueFormat( bigValueStore, bigValueCommandConsumer, buffer );
        value.writeTo( format );
        buffer.flip();
        return buffer;
    }
}
