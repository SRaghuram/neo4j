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
import static org.neo4j.internal.freki.MutableNodeRecordData.FLAG_RELATIONSHIPS;
import static org.neo4j.internal.freki.MutableNodeRecordData.buildRecordPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.SINGLE_VLONG_MAX_SIZE;

/**
 * Contains all logic about making graph data changes to a Freki store, everything from loading and modifying data to serializing
 * and overflowing into larger records or dense store.
 */
class GraphUpdates
{
    private final Collection<StorageCommand> bigValueCommands;
    private final Consumer<StorageCommand> bigValueCommandConsumer;
    private final MutableLongObjectMap<NodeUpdates> mutations = LongObjectMaps.mutable.empty();
    private final MainStores stores;
    private final PageCursorTracer cursorTracer;

    //Intermediate buffer slots
    static final int PROPERTIES = Header.OFFSET_PROPERTIES;
    static final int RELATIONSHIPS = Header.OFFSET_RELATIONSHIPS;
    static final int DEGREES = Header.OFFSET_DEGREES;
    static final int RELATIONSHIPS_OFFSETS = Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS;
    static final int NEXT_INTERNAL_RELATIONSHIP_ID = Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID;
    static final int RECORD_POINTER = Header.OFFSET_RECORD_POINTER;
    static final int LABELS = Header.NUM_OFFSETS;

    GraphUpdates( MainStores stores, PageCursorTracer cursorTracer )
    {
        this( stores, new ArrayList<>(), null, cursorTracer );
    }

    GraphUpdates( MainStores stores, Collection<StorageCommand> bigValueCommands,
            Consumer<StorageCommand> bigValueCommandConsumer, PageCursorTracer cursorTracer )
    {
        this.stores = stores;
        this.cursorTracer = cursorTracer;
        this.bigValueCommands = bigValueCommands;
        this.bigValueCommandConsumer = bigValueCommandConsumer != null ? bigValueCommandConsumer : bigValueCommands::add;
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
        ByteBuffer[] intermediateBuffers = new ByteBuffer[Header.NUM_OFFSETS + 1];
        int x8Size = stores.largestMainStore().recordDataSize();
        intermediateBuffers[PROPERTIES] = ByteBuffer.wrap( new byte[x8Size] );
        intermediateBuffers[RELATIONSHIPS] = ByteBuffer.wrap( new byte[x8Size] );
        intermediateBuffers[DEGREES] = ByteBuffer.wrap( new byte[x8Size] );
        intermediateBuffers[RELATIONSHIPS_OFFSETS] = ByteBuffer.wrap( new byte[x8Size] );
        intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID] = ByteBuffer.wrap( new byte[SINGLE_VLONG_MAX_SIZE] );
        intermediateBuffers[RECORD_POINTER] = ByteBuffer.wrap( new byte[SINGLE_VLONG_MAX_SIZE] );
        intermediateBuffers[LABELS] = ByteBuffer.wrap( new byte[x8Size] );

        ByteBuffer smallBuffer = ByteBuffer.wrap( new byte[stores.mainStore.recordDataSize()] );
        ByteBuffer maxBuffer = ByteBuffer.wrap( new byte[x8Size] );
        Header x1Header = new Header();
        Header xLHeader = new Header();
        for ( NodeUpdates mutation : mutations )
        {
            mutation.serialize( smallBuffer, maxBuffer, intermediateBuffers, otherCommands::add, x1Header, xLHeader );
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
                sparse.data.copyDataFrom( largeData );
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

        void serialize( ByteBuffer smallBuffer, ByteBuffer maxBuffer, ByteBuffer[] intermediateBuffers, Consumer<StorageCommand> otherCommands, Header x1Header,
                Header xLHeader )
                throws ConstraintViolationTransactionFailureException
        {
            prepareForCommandExtraction();

            if ( deleted )
            {
                deletionCommands( otherCommands );
                return;
            }

            smallBuffer.clear();
            maxBuffer.clear();
            for ( ByteBuffer buffer : intermediateBuffers )
            {
                buffer.clear();
            }

            boolean isDense = sparse.data.serializeMainData( intermediateBuffers, stores.bigPropertyValueStore, bigValueCommandConsumer );
            intermediateBuffers[LABELS].flip();
            intermediateBuffers[PROPERTIES].flip();
            intermediateBuffers[RELATIONSHIPS].flip();
            intermediateBuffers[RELATIONSHIPS_OFFSETS].flip();
            intermediateBuffers[DEGREES].flip();
            int labelsSize = intermediateBuffers[LABELS].limit();
            int propsSize = intermediateBuffers[PROPERTIES].limit();
            int relsSize = intermediateBuffers[RELATIONSHIPS].limit() + intermediateBuffers[RELATIONSHIPS_OFFSETS].limit();
            int degreesSize = intermediateBuffers[DEGREES].limit();
            x1Header.clearOffsetMarksAndFlags();
            if ( !isDense )
            {
                // Then at least see if the combined parts are larger than x8
                x1Header.setFlag( Header.FLAG_LABELS, labelsSize > 0 );
                x1Header.markHasOffset( Header.OFFSET_PROPERTIES, propsSize > 0 );
                x1Header.markHasOffset( Header.OFFSET_RELATIONSHIPS, relsSize > 0 );
                x1Header.markHasOffset( Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS, relsSize > 0 );
                x1Header.markHasOffset( Header.OFFSET_RECORD_POINTER );
                if ( labelsSize + propsSize + relsSize + x1Header.spaceNeeded() + SINGLE_VLONG_MAX_SIZE > stores.largestMainStore().recordDataSize() )
                {
                    // We _flip to dense_ a bit earlier than absolutely optimal, but after that the x8 record can be used for other things
                    isDense = true;
                }
            }

            if ( isDense )
            {
                if ( dense == null )
                {
                    moveDataToDense();
                    intermediateBuffers[RELATIONSHIPS_OFFSETS].clear().flip();
                    intermediateBuffers[RELATIONSHIPS].clear().flip();
                    relsSize = 0;

                    sparse.data.serializeDegrees( intermediateBuffers[DEGREES].clear() );
                    intermediateBuffers[DEGREES].flip();
                    degreesSize = intermediateBuffers[DEGREES].limit();
                }
                dense.createCommands( otherCommands );
            }

            // X LEGO TIME
            int miscSize = 0;
            x1Header.clearOffsetMarksAndFlags();
            x1Header.setFlag( Header.FLAG_LABELS, labelsSize > 0 );
            x1Header.markHasOffset( Header.OFFSET_PROPERTIES, propsSize > 0 );
            int nextInternalRelIdSize = 0;
            if ( isDense )
            {
                x1Header.setFlag( Header.FLAG_IS_DENSE );
                x1Header.markHasOffset( Header.OFFSET_DEGREES );
                x1Header.markHasOffset( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
                sparse.data.serializeNextInternalRelationshipId( intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID] );
                intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].flip();
                nextInternalRelIdSize = intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].limit();
                miscSize += nextInternalRelIdSize;
            }
            else if ( relsSize > 0 )
            {
                x1Header.markHasOffset( Header.OFFSET_RELATIONSHIPS );
                x1Header.markHasOffset( Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
            }

            if ( x1Header.spaceNeeded() + labelsSize + propsSize + Math.max( relsSize, degreesSize ) + miscSize <= stores.mainStore.recordDataSize() )
            {
                //WE FIT IN x1
                serializeParts( smallBuffer, intermediateBuffers, x1Header );
                x1Command( smallBuffer, otherCommands );
                return;
            }

            //we did not fit in x1 only, fit as many things as possible in x1
            int worstCaseMiscSize = miscSize + SINGLE_VLONG_MAX_SIZE;

            x1Header.clearOffsetMarksAndFlags();
            // build x1 header
            x1Header.markHasOffset( Header.OFFSET_RECORD_POINTER );
            x1Header.setFlag( Header.FLAG_IS_DENSE, isDense );
            int spaceLeftInX1 = stores.mainStore.recordDataSize() - worstCaseMiscSize;
            spaceLeftInX1 = tryKeepInX1Flag( x1Header, labelsSize, spaceLeftInX1, Header.FLAG_LABELS );
            spaceLeftInX1 = tryKeepInX1( x1Header, nextInternalRelIdSize, spaceLeftInX1, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            spaceLeftInX1 = tryKeepInX1( x1Header, propsSize, spaceLeftInX1, Header.OFFSET_PROPERTIES );
            spaceLeftInX1 = tryKeepInX1( x1Header, degreesSize, spaceLeftInX1, Header.OFFSET_DEGREES );
            tryKeepInX1( x1Header, relsSize, spaceLeftInX1, Header.OFFSET_RELATIONSHIPS, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );

            // build xL header and serialize
            xLHeader.clearOffsetMarksAndFlags();
            xLHeader.setFlag( Header.FLAG_IS_DENSE, isDense );
            prepareRecordPointer( xLHeader, intermediateBuffers[RECORD_POINTER], buildRecordPointer( 0, nodeId ) );
            movePartToXLFlag( x1Header, xLHeader, labelsSize, Header.FLAG_LABELS );
            movePartToXL( x1Header, xLHeader, propsSize, Header.OFFSET_PROPERTIES );
            movePartToXL( x1Header, xLHeader, degreesSize, Header.OFFSET_DEGREES );
            movePartToXL( x1Header, xLHeader, nextInternalRelIdSize, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            movePartToXL( x1Header, xLHeader, relsSize, Header.OFFSET_RELATIONSHIPS );
            movePartToXL( x1Header, xLHeader, relsSize, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
            serializeParts( maxBuffer, intermediateBuffers, xLHeader );
            SimpleStore xLStore = stores.storeSuitableForRecordSize( maxBuffer.limit(), 1 );
            long forwardPointer = xLargeCommands( maxBuffer, xLStore, otherCommands );

            // serialize x1
            prepareRecordPointer( x1Header, intermediateBuffers[RECORD_POINTER], forwardPointer );
            serializeParts( smallBuffer, intermediateBuffers, x1Header );
            x1Command( smallBuffer, otherCommands );
        }

        private void deletionCommands( Consumer<StorageCommand> otherCommands )
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
        }

        private void prepareRecordPointer( Header header, ByteBuffer intermediateBuffer, long recordPointer )
        {
            intermediateBuffer.clear();
            header.markHasOffset( Header.OFFSET_RECORD_POINTER );
            sparse.data.setRecordPointer( recordPointer );
            sparse.data.serializeRecordPointer( intermediateBuffer );
            intermediateBuffer.flip();
        }

        private void movePartToXLFlag( Header header, Header xlHeader, int partSize, int flag )
        {
            xlHeader.setFlag( flag, partSize > 0 && !header.hasFlag( flag ) );
        }

        private void movePartToXL( Header header, Header xlHeader, int partSize, int offset )
        {
            xlHeader.markHasOffset( offset, partSize > 0 && !header.hasOffset( offset ) );
        }

        private int tryKeepInX1Flag( Header header, int labelsSize, int spaceLeft, int flag )
        {
            if ( labelsSize > 0 )
            {
                header.setFlag( flag );
                if ( labelsSize <= spaceLeft - header.spaceNeeded() )
                {
                    // ok properties can live in x1
                    spaceLeft -= labelsSize;
                }
                else
                {
                    header.removeFlag( flag );
                }
            }
            return spaceLeft;
        }

        private int tryKeepInX1( Header header, int partSize, int spaceLeftInX1, int... headerOffsets )
        {
            if ( partSize > 0 )
            {
                for ( int headerOffset : headerOffsets )
                {
                    header.markHasOffset( headerOffset );
                }
                if ( partSize <= spaceLeftInX1 - header.spaceNeeded() )
                {
                    // ok this part can live in x1
                    spaceLeftInX1 -= partSize;
                }
                else
                {
                    for ( int headerOffset : headerOffsets )
                    {
                        header.unmarkHasOffset( headerOffset );
                    }
                }
            }
            return spaceLeftInX1;
        }

        private static void serializeParts( ByteBuffer into, ByteBuffer[] intermediateBuffers, Header header )
        {
            header.allocateSpace( into );
            if ( header.hasFlag( Header.FLAG_LABELS ) )
            {
                into.put( intermediateBuffers[LABELS] );
            }
            serializePart( into, intermediateBuffers[PROPERTIES], header, Header.OFFSET_PROPERTIES );
            serializePart( into, intermediateBuffers[RELATIONSHIPS], header, Header.OFFSET_RELATIONSHIPS );
            serializePart( into, intermediateBuffers[RELATIONSHIPS_OFFSETS], header, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
            serializePart( into, intermediateBuffers[DEGREES], header, Header.OFFSET_DEGREES );
            serializePart( into, intermediateBuffers[RECORD_POINTER], header, Header.OFFSET_RECORD_POINTER );
            serializePart( into, intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID], header, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            int endPosition = into.position();
            header.serialize( into.position( 0 ) );
            into.position( endPosition ).flip();
        }

        private static void serializePart( ByteBuffer into, ByteBuffer part, Header header, int headerOffsetSlot )
        {
            if ( header.hasOffset( headerOffsetSlot ) )
            {
                header.setOffset( headerOffsetSlot, into.position() );
                into.put( part );
            }
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

        /**
         * @return the existing or allocated xL ID into sparse.data so that X1 can be serialized afterwards
         */
        private long xLargeCommands( ByteBuffer maxBuffer, SimpleStore store, Consumer<StorageCommand> commands )
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

            return buildRecordPointer( after.sizeExp(), after.id );
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
            data.setDense( true );
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
