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
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import java.nio.ByteBuffer;
import java.util.ArrayList;
<<<<<<< HEAD
=======
import java.util.Arrays;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
<<<<<<< HEAD
=======
import java.util.function.IntPredicate;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec

import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationships;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
<<<<<<< HEAD
=======
import org.neo4j.memory.MemoryTracker;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeData.buildRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.idFromRecordPointer;
<<<<<<< HEAD
import static org.neo4j.internal.freki.MutableNodeData.serializeRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
=======
import static org.neo4j.internal.freki.MutableNodeData.recordPointerToString;
import static org.neo4j.internal.freki.MutableNodeData.serializeRecordPointers;
import static org.neo4j.internal.freki.MutableNodeData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.DUAL_VLONG_MAX_SIZE;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
import static org.neo4j.internal.freki.StreamVByte.SINGLE_VLONG_MAX_SIZE;

/**
 * Contains all logic about making graph data changes to a Freki store, everything from loading and modifying data to serializing
 * and overflowing into larger records or dense store.
 */
class GraphUpdates
{
<<<<<<< HEAD
    private final Collection<StorageCommand> bigValueCommands;
=======
    private static final int WORST_CASE_HEADER_AND_STUFF_SIZE = Header.WORST_CASE_SIZE + DUAL_VLONG_MAX_SIZE;
    private final Collection<StorageCommand> bigValueCommands;
    private final MemoryTracker memoryTracker;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
    private final Consumer<StorageCommand> bigValueCommandConsumer;
    private final MutableLongObjectMap<NodeUpdates> mutations = LongObjectMaps.mutable.empty();
    private final MainStores stores;
    private final PageCursorTracer cursorTracer;

    //Intermediate buffer slots
<<<<<<< HEAD
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
=======
    static final int PROPERTIES = 0;
    static final int RELATIONSHIPS = 1;
    static final int DEGREES = 2;
    static final int RELATIONSHIPS_OFFSETS = 3;
    static final int NEXT_INTERNAL_RELATIONSHIP_ID = 4;
    static final int LABELS = 5;
    static final int NUM_BUFFERS = LABELS + 1;

    GraphUpdates( MainStores stores, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this( stores, new ArrayList<>(), null, cursorTracer, memoryTracker );
    }

    GraphUpdates( MainStores stores, Collection<StorageCommand> bigValueCommands,
            Consumer<StorageCommand> bigValueCommandConsumer, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
    {
        this.stores = stores;
        this.cursorTracer = cursorTracer;
        this.bigValueCommands = bigValueCommands;
<<<<<<< HEAD
=======
        this.memoryTracker = memoryTracker;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
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
        NodeUpdates updates = new NodeUpdates( nodeId, stores, bigValueCommandConsumer, cursorTracer );
<<<<<<< HEAD
        updates.create();
=======
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        mutations.put( nodeId, updates );
    }

    void extractUpdates( Consumer<StorageCommand> commands ) throws ConstraintViolationTransactionFailureException
    {
        List<StorageCommand> otherCommands = new ArrayList<>();
<<<<<<< HEAD
        ByteBuffer[] intermediateBuffers = new ByteBuffer[Header.NUM_OFFSETS + 1];
        int x8Size = stores.largestMainStore().recordDataSize();
        intermediateBuffers[PROPERTIES] = ByteBuffer.wrap( new byte[x8Size] );
        intermediateBuffers[RELATIONSHIPS] = ByteBuffer.wrap( new byte[x8Size] );
        intermediateBuffers[DEGREES] = ByteBuffer.wrap( new byte[x8Size] );
        intermediateBuffers[RELATIONSHIPS_OFFSETS] = ByteBuffer.wrap( new byte[x8Size] );
        intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID] = ByteBuffer.wrap( new byte[SINGLE_VLONG_MAX_SIZE] );
        intermediateBuffers[RECORD_POINTER] = ByteBuffer.wrap( new byte[SINGLE_VLONG_MAX_SIZE] );
        intermediateBuffers[LABELS] = ByteBuffer.wrap( new byte[x8Size] );
=======
        IntermediateBuffer[] intermediateBuffers = new IntermediateBuffer[NUM_BUFFERS];
        int x8Size = stores.largestMainStore().recordDataSize();
        int x8EffectiveSize = x8Size - WORST_CASE_HEADER_AND_STUFF_SIZE;
        intermediateBuffers[PROPERTIES] = new IntermediateBuffer( x8EffectiveSize );
        intermediateBuffers[RELATIONSHIPS] = new IntermediateBuffer( x8EffectiveSize );
        intermediateBuffers[DEGREES] = new IntermediateBuffer( x8EffectiveSize );
        intermediateBuffers[RELATIONSHIPS_OFFSETS] = new IntermediateBuffer( x8EffectiveSize );
        intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID] = new IntermediateBuffer( SINGLE_VLONG_MAX_SIZE );
        ByteBuffer recordPointersBuffer = ByteBuffer.wrap( new byte[DUAL_VLONG_MAX_SIZE] );
        intermediateBuffers[LABELS] = new IntermediateBuffer( x8EffectiveSize );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec

        ByteBuffer smallBuffer = ByteBuffer.wrap( new byte[stores.mainStore.recordDataSize()] );
        ByteBuffer maxBuffer = ByteBuffer.wrap( new byte[x8Size] );
        Header x1Header = new Header();
        Header xLHeader = new Header();
        for ( NodeUpdates mutation : mutations )
        {
<<<<<<< HEAD
            mutation.serialize( smallBuffer, maxBuffer, intermediateBuffers, otherCommands::add, x1Header, xLHeader );
=======
            mutation.serialize( smallBuffer, maxBuffer, intermediateBuffers, recordPointersBuffer, otherCommands::add, x1Header, xLHeader );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
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

        abstract void updateRelationshipProperties( long internalId, int type, long otherNode, boolean outgoing,
                Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed );

        abstract void delete();
    }

    static class NodeUpdates extends NodeDataModifier
    {
        private final long nodeId;
        private final MainStores stores;
        private final Consumer<StorageCommand> bigValueCommandConsumer;
        private final PageCursorTracer cursorTracer;

        // the before-state
<<<<<<< HEAD
        private Record x1Before;
        private Record xLBefore;
=======
        private RecordChain firstBeforeRecord;
        private RecordChain lastBeforeRecord;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec

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
            this.sparse = new SparseRecordAndData( nodeId, stores, cursorTracer ); // this will always exist
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

<<<<<<< HEAD
            MutableNodeData x1Data = sparse.add( x1 );
            x1Before = x1;

            long recordPointer = x1Data.getRecordPointer();
            if ( recordPointer != NULL )
            {
                Record xL = readRecord( stores, sizeExponentialFromRecordPointer( recordPointer ), idFromRecordPointer( recordPointer ), cursorTracer );
                sparse.add( xL );
                xLBefore = xL;
            }
            if ( x1Data.isDense() )
=======
            MutableNodeData data = sparse.add( x1 );
            firstBeforeRecord = lastBeforeRecord = new RecordChain( x1 );
            long fwPointer;
            while ( (fwPointer = data.getLastLoadedForwardPointer()) != NULL )
            {
                Record xL = readRecord( stores, sizeExponentialFromRecordPointer( fwPointer ), idFromRecordPointer( fwPointer ), cursorTracer );
                if ( xL == null )
                {
                    throw new IllegalStateException( x1 + " points to " + recordPointerToString( fwPointer ) + " that isn't in use" );
                }
                sparse.add( xL );
                RecordChain chain = new RecordChain( xL );
                chain.next = firstBeforeRecord;
                firstBeforeRecord = chain;
            }
            if ( data.isDense() )
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            {
                dense = new DenseRecordAndData( sparse, stores.denseStore, stores.bigPropertyValueStore, bigValueCommandConsumer, cursorTracer );
            }
        }

<<<<<<< HEAD
        void create()
        {
            sparse.addEmpty();
        }

=======
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
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
        void updateRelationshipProperties( long internalId, int type, long otherNode, boolean outgoing, Iterable<StorageProperty> added,
                Iterable<StorageProperty> changed, IntIterable removed )
        {
            forRelationships().updateRelationshipProperties( internalId, type, otherNode, outgoing, added, changed, removed );
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
<<<<<<< HEAD
        }

        void serialize( ByteBuffer smallBuffer, ByteBuffer maxBuffer, ByteBuffer[] intermediateBuffers, Consumer<StorageCommand> otherCommands, Header x1Header,
                Header xLHeader )
                throws ConstraintViolationTransactionFailureException
=======
            // Reset the positions of the before buffers
            // assumption: MutableNodeData has already looked at the header and set the limit to the end of the effective data
            for ( RecordChain chain = firstBeforeRecord; chain != null; chain = chain.next )
            {
                chain.record.data( 0 );
            }
        }

        void serialize( ByteBuffer smallBuffer, ByteBuffer maxBuffer, IntermediateBuffer[] intermediateBuffers, ByteBuffer recordPointersBuffer,
                Consumer<StorageCommand> otherCommands, Header x1Header, Header xLHeader ) throws ConstraintViolationTransactionFailureException
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        {
            prepareForCommandExtraction();

            if ( deleted )
            {
                deletionCommands( otherCommands );
                return;
            }

            smallBuffer.clear();
            maxBuffer.clear();
<<<<<<< HEAD
            for ( ByteBuffer buffer : intermediateBuffers )
=======
            for ( IntermediateBuffer buffer : intermediateBuffers )
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            {
                buffer.clear();
            }

<<<<<<< HEAD
            boolean isDense = false;
            for ( MutableNodeData data : sparse.datas )
            {
                isDense |= data.serializeMainData( intermediateBuffers, stores.bigPropertyValueStore, bigValueCommandConsumer );
            }

=======
            boolean isDense = sparse.data.serializeMainData( intermediateBuffers, stores.bigPropertyValueStore, bigValueCommandConsumer );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            intermediateBuffers[LABELS].flip();
            intermediateBuffers[PROPERTIES].flip();
            intermediateBuffers[RELATIONSHIPS].flip();
            intermediateBuffers[RELATIONSHIPS_OFFSETS].flip();
            intermediateBuffers[DEGREES].flip();
<<<<<<< HEAD
            int labelsSize = intermediateBuffers[LABELS].limit();
            int propsSize = intermediateBuffers[PROPERTIES].limit();
            int relsSize = intermediateBuffers[RELATIONSHIPS].limit() + intermediateBuffers[RELATIONSHIPS_OFFSETS].limit();
            int degreesSize = intermediateBuffers[DEGREES].limit();
            x1Header.clearMarks();
            if ( !isDense )
            {
                // Then at least see if the combined parts are larger than x8
                x1Header.mark( Header.FLAG_LABELS, labelsSize > 0 );
                x1Header.mark( Header.OFFSET_PROPERTIES, propsSize > 0 );
                x1Header.mark( Header.OFFSET_RELATIONSHIPS, relsSize > 0 );
                x1Header.mark( Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS, relsSize > 0 );
                x1Header.mark( Header.OFFSET_RECORD_POINTER, true );
                if ( labelsSize + propsSize + relsSize + x1Header.spaceNeeded() + SINGLE_VLONG_MAX_SIZE > stores.largestMainStore().recordDataSize() )
                {
                    // We _flip to dense_ a bit earlier than absolutely optimal, but after that the x8 record can be used for other things
                    isDense = true;
                }
            }

=======
            int relsSize = intermediateBuffers[RELATIONSHIPS].limit() + intermediateBuffers[RELATIONSHIPS_OFFSETS].limit();
            isDense |= relsSize > intermediateBuffers[RELATIONSHIPS].capacity();
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            if ( isDense )
            {
                if ( dense == null )
                {
                    moveDataToDense();
                    intermediateBuffers[RELATIONSHIPS_OFFSETS].clear().flip();
                    intermediateBuffers[RELATIONSHIPS].clear().flip();
                    relsSize = 0;

<<<<<<< HEAD
                    sparse.dataFor( Header.OFFSET_DEGREES ).serializeDegrees( intermediateBuffers[DEGREES].clear() );
                    intermediateBuffers[DEGREES].flip();
                    degreesSize = intermediateBuffers[DEGREES].limit();
=======
                    sparse.data.serializeDegrees( intermediateBuffers[DEGREES].clear() );
                    intermediateBuffers[DEGREES].flip();
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
                }
                dense.createCommands( otherCommands );
            }

            // X LEGO TIME
            int miscSize = 0;
            x1Header.clearMarks();
<<<<<<< HEAD
            x1Header.mark( Header.FLAG_LABELS, labelsSize > 0 );
            x1Header.mark( Header.OFFSET_PROPERTIES, propsSize > 0 );
            int nextInternalRelIdSize = 0;
=======
            x1Header.mark( Header.OFFSET_END, true );
            x1Header.mark( Header.FLAG_LABELS, intermediateBuffers[LABELS].totalSize() > 0 );
            x1Header.mark( Header.OFFSET_PROPERTIES, intermediateBuffers[PROPERTIES].totalSize() > 0 );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            if ( isDense )
            {
                x1Header.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, true );
                x1Header.mark( Header.OFFSET_DEGREES, true );
                x1Header.mark( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID, true );
<<<<<<< HEAD
                sparse.dataFor( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ).serializeNextInternalRelationshipId(
                        intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID] );
                intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].flip();
                nextInternalRelIdSize = intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].limit();
                miscSize += nextInternalRelIdSize;
=======
                sparse.data.serializeNextInternalRelationshipId( intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].add() );
                intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].flip();
                miscSize += intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].totalSize();
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            }
            else if ( relsSize > 0 )
            {
                x1Header.mark( Header.OFFSET_RELATIONSHIPS, true );
                x1Header.mark( Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS, true );
            }

<<<<<<< HEAD
            if ( x1Header.spaceNeeded() + labelsSize + propsSize + Math.max( relsSize, degreesSize ) + miscSize <= stores.mainStore.recordDataSize() )
            {
                //WE FIT IN x1
                serializeParts( smallBuffer, intermediateBuffers, x1Header, null );
                x1Command( smallBuffer, otherCommands );
                if ( xLBefore != null )
                {
                    otherCommands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, deletedVersionOf( xLBefore ) ) );
                }
=======
            FrekiCommand.SparseNode command = new FrekiCommand.SparseNode( nodeId );
            if ( x1Header.spaceNeeded() + intermediateBuffers[LABELS].totalSize() + intermediateBuffers[PROPERTIES].totalSize() +
                    Math.max( relsSize, intermediateBuffers[DEGREES].totalSize() ) + miscSize <= stores.mainStore.recordDataSize() )
            {
                //WE FIT IN x1
                serializeParts( smallBuffer, intermediateBuffers, recordPointersBuffer, x1Header );
                addX1Record( smallBuffer, command );
                deleteRemainingBeforeRecords( command );
                otherCommands.accept( command );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
                return;
            }

            //we did not fit in x1 only, fit as many things as possible in x1
            int worstCaseMiscSize = miscSize + SINGLE_VLONG_MAX_SIZE;

            x1Header.clearMarks();
            // build x1 header
<<<<<<< HEAD
            x1Header.mark( Header.OFFSET_RECORD_POINTER, true );
            x1Header.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, isDense );
            int spaceLeftInX1 = stores.mainStore.recordDataSize() - worstCaseMiscSize;
            spaceLeftInX1 = tryKeepInX1( x1Header, labelsSize, spaceLeftInX1, Header.FLAG_LABELS );
            spaceLeftInX1 = tryKeepInX1( x1Header, nextInternalRelIdSize, spaceLeftInX1, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            spaceLeftInX1 = tryKeepInX1( x1Header, propsSize, spaceLeftInX1, Header.OFFSET_PROPERTIES );
            spaceLeftInX1 = tryKeepInX1( x1Header, degreesSize, spaceLeftInX1, Header.OFFSET_DEGREES );
            tryKeepInX1( x1Header, relsSize, spaceLeftInX1, Header.OFFSET_RELATIONSHIPS, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );

            // build xL header and serialize
            xLHeader.clearMarks();
            xLHeader.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, isDense );
            prepareRecordPointer( xLHeader, intermediateBuffers[RECORD_POINTER], buildRecordPointer( 0, nodeId ) );
            movePartToXL( x1Header, xLHeader, labelsSize, Header.FLAG_LABELS );
            movePartToXL( x1Header, xLHeader, propsSize, Header.OFFSET_PROPERTIES );
            movePartToXL( x1Header, xLHeader, degreesSize, Header.OFFSET_DEGREES );
            movePartToXL( x1Header, xLHeader, nextInternalRelIdSize, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            movePartToXL( x1Header, xLHeader, relsSize, Header.OFFSET_RELATIONSHIPS );
            movePartToXL( x1Header, xLHeader, relsSize, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
            serializeParts( maxBuffer, intermediateBuffers, xLHeader, x1Header );
            SimpleStore xLStore = stores.storeSuitableForRecordSize( maxBuffer.limit(), 1 );
            long forwardPointer = xLargeCommands( maxBuffer, xLStore, otherCommands );

            // serialize x1
            prepareRecordPointer( x1Header, intermediateBuffers[RECORD_POINTER], forwardPointer );
            serializeParts( smallBuffer, intermediateBuffers, x1Header, xLHeader );
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
            header.mark( Header.OFFSET_RECORD_POINTER, true );
            serializeRecordPointer( intermediateBuffer, recordPointer );
=======
            x1Header.mark( Header.OFFSET_END, true );
            x1Header.mark( Header.OFFSET_RECORD_POINTER, true );
            x1Header.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, isDense );
            int spaceLeftInX1 = stores.mainStore.recordDataSize() - worstCaseMiscSize;
            spaceLeftInX1 = tryKeepInX1( x1Header, intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].currentSize(), spaceLeftInX1,
                    Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            spaceLeftInX1 = tryKeepInX1( x1Header, intermediateBuffers[LABELS].currentSize(), spaceLeftInX1, Header.FLAG_LABELS );
            spaceLeftInX1 = tryKeepInX1( x1Header, intermediateBuffers[DEGREES].currentSize(), spaceLeftInX1, Header.OFFSET_DEGREES );
            spaceLeftInX1 = tryKeepInX1( x1Header, intermediateBuffers[PROPERTIES].currentSize(), spaceLeftInX1, Header.OFFSET_PROPERTIES );
            tryKeepInX1( x1Header, relsSize, spaceLeftInX1, Header.OFFSET_RELATIONSHIPS, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );

            // build xLChain header
            xLHeader.clearMarks();
            xLHeader.mark( Header.OFFSET_END, true );
            xLHeader.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, isDense );

            movePartToXL( x1Header, xLHeader, intermediateBuffers[LABELS].currentSize(), Header.FLAG_LABELS );
            movePartToXL( x1Header, xLHeader, intermediateBuffers[PROPERTIES].currentSize(), Header.OFFSET_PROPERTIES );
            movePartToXL( x1Header, xLHeader, intermediateBuffers[DEGREES].currentSize(), Header.OFFSET_DEGREES );
            movePartToXL( x1Header, xLHeader, intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].currentSize(), Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            movePartToXL( x1Header, xLHeader, relsSize, Header.OFFSET_RELATIONSHIPS );
            movePartToXL( x1Header, xLHeader, relsSize, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );

            //Now we know whats in X1 and XL(chain), update their references.
            x1Header.setReference( xLHeader );
            xLHeader.setReference( x1Header );

            //split chain header into individual headers
            boolean canFitInSingleXL = (xLHeader.hasMark( Header.FLAG_LABELS ) ? intermediateBuffers[LABELS].totalSize() : 0) +
                    (xLHeader.hasMark( Header.OFFSET_PROPERTIES ) ? intermediateBuffers[PROPERTIES].totalSize() : 0) +
                    (xLHeader.hasMark( Header.OFFSET_RELATIONSHIPS ) ? relsSize : 0) +
                    (xLHeader.hasMark( Header.OFFSET_DEGREES ) ? intermediateBuffers[DEGREES].totalSize() : 0) +
                    (xLHeader.hasMark( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ) ?
                            intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].totalSize() : 0) + xLHeader.spaceNeeded() +
                    DUAL_VLONG_MAX_SIZE <= stores.largestMainStore().recordDataSize();

            long forwardPointer = NULL;
            long backwardPointer = buildRecordPointer( 0, nodeId );
            if ( !canFitInSingleXL )
            {
                Header chainHeader = Header.shallowCopy( xLHeader );
                //Unmark common offsets for each link
                chainHeader.mark( Header.OFFSET_END, false );
                chainHeader.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, false );

                List<Header> xLChain = new ArrayList<>();
                worstCaseMiscSize = miscSize + 2 * SINGLE_VLONG_MAX_SIZE;
                final int xLMaxSize = stores.largestMainStore().recordDataSize() - worstCaseMiscSize;
                while ( chainHeader.hasMarkers() ) //we try to move everything away from chain header into links
                {
                    Header linkHeader = new Header();
                    linkHeader.mark( Header.OFFSET_END, true );
                    linkHeader.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, isDense );
                    linkHeader.mark( Header.OFFSET_RECORD_POINTER, true );

                    int spaceLeft = xLMaxSize;
                    spaceLeft = tryPutInXLLink( chainHeader, linkHeader, intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID],
                            intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].currentSize(), spaceLeft, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
                    spaceLeft = tryPutInXLLink( chainHeader, linkHeader, intermediateBuffers[LABELS], intermediateBuffers[LABELS].currentSize(), spaceLeft,
                            Header.FLAG_LABELS );
                    spaceLeft = tryPutInXLLink( chainHeader, linkHeader, intermediateBuffers[DEGREES], intermediateBuffers[DEGREES].currentSize(), spaceLeft,
                            Header.OFFSET_DEGREES );
                    spaceLeft = tryPutInXLLink( chainHeader, linkHeader, intermediateBuffers[PROPERTIES], intermediateBuffers[PROPERTIES].currentSize(),
                            spaceLeft, Header.OFFSET_PROPERTIES );
                    spaceLeft = tryPutInXLLink( chainHeader, linkHeader, intermediateBuffers[RELATIONSHIPS], relsSize, spaceLeft, Header.OFFSET_RELATIONSHIPS,
                            Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
                    xLChain.add( linkHeader );

                    if ( spaceLeft == xLMaxSize )
                    {
                        // We did not put anything in this link, likely some single part that does not fit into a single (max-size) record. Should not happen!
                        String msg = "Splitting XL into chain failed. Chain:%s, Link%s. Labels(%d), Props(%d), Degrees(%d), NextRelId(%d), Rels(%d)";
                        throw new IllegalStateException( String.format( msg, chainHeader, linkHeader, intermediateBuffers[LABELS].currentSize(),
                                intermediateBuffers[PROPERTIES].currentSize(), intermediateBuffers[DEGREES].currentSize(),
                                intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].currentSize(), relsSize ) );
                    }
                }

                for ( int i = xLChain.size() - 1; i >= 0; i-- )
                {
                    Header linkHeader = xLChain.get( i );
                    long[] pointers = forwardPointer != NULL ? new long[]{backwardPointer, forwardPointer} : new long[]{backwardPointer};
                    prepareRecordPointer( linkHeader, recordPointersBuffer, pointers );

                    maxBuffer.clear();
                    linkHeader.setReference( x1Header );
                    serializeParts( maxBuffer, intermediateBuffers, recordPointersBuffer, linkHeader );
                    SimpleStore xLStore = stores.storeSuitableForRecordSize( maxBuffer.limit(), 1 );
                    forwardPointer = addXLRecords( maxBuffer, xLStore, command );
                }
            }
            else
            {
                prepareRecordPointer( xLHeader, recordPointersBuffer, backwardPointer );
                serializeParts( maxBuffer, intermediateBuffers, recordPointersBuffer, xLHeader );
                SimpleStore xLStore = stores.storeSuitableForRecordSize( maxBuffer.limit(), 1 );
                forwardPointer = addXLRecords( maxBuffer, xLStore, command );
            }

            // serialize x1
            prepareRecordPointer( x1Header, recordPointersBuffer, forwardPointer );
            serializeParts( smallBuffer, intermediateBuffers, recordPointersBuffer, x1Header );
            addX1Record( smallBuffer, command );
            deleteRemainingBeforeRecords( command );
            otherCommands.accept( command );
        }

        private void deleteRemainingBeforeRecords( FrekiCommand.SparseNode node )
        {
            for ( RecordChain chain = firstBeforeRecord; chain != null; chain = chain.next )
            {
                node.addChange( chain.record, null );
            }
            firstBeforeRecord = lastBeforeRecord = null;
        }

        private void deletionCommands( Consumer<StorageCommand> otherCommands )
        {
            FrekiCommand.SparseNode node = new FrekiCommand.SparseNode( nodeId );
            deleteRemainingBeforeRecords( node );
            otherCommands.accept( node );
        }

        private void prepareRecordPointer( Header header, ByteBuffer intermediateBuffer, long... recordPointers )
        {
            intermediateBuffer.clear();
            header.mark( Header.OFFSET_RECORD_POINTER, true );
            serializeRecordPointers( intermediateBuffer, recordPointers );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            intermediateBuffer.flip();
        }

        private void movePartToXL( Header header, Header xlHeader, int partSize, int offset )
        {
            xlHeader.mark( offset, partSize > 0 && !header.hasMark( offset ) );
        }

        private int tryKeepInX1( Header header, int partSize, int spaceLeftInX1, int... slots )
        {
            if ( partSize > 0 )
            {
                for ( int slot : slots )
                {
                    header.mark( slot, true );
                }
                if ( partSize <= spaceLeftInX1 - header.spaceNeeded() )
                {
                    // ok this part can live in x1
                    spaceLeftInX1 -= partSize;
                }
                else
                {
                    for ( int slot : slots )
                    {
                        header.mark( slot, false );
                    }
                }
            }
            return spaceLeftInX1;
        }

<<<<<<< HEAD
        private static void serializeParts( ByteBuffer into, ByteBuffer[] intermediateBuffers, Header header, Header referenceHeader )
=======
        private int tryPutInXLLink( Header chainHeader, Header linkHeader, IntermediateBuffer intermediateBuffer, int partSize, int spaceLeftInXL,
                int... slots )
        {
            if ( partSize > 0 && chainHeader.hasMark( slots[0] ) )
            {
                // Tentatively mark the link header as if we're sure it'll fit, to calculate the header space needed correctly
                for ( int slot : slots )
                {
                    linkHeader.mark( slot, true );
                }
                if ( partSize <= spaceLeftInXL - linkHeader.spaceNeeded() )
                {
                    spaceLeftInXL -= partSize;
                    boolean lastBuffer = !intermediateBuffer.next();
                    if ( lastBuffer )
                    {
                        // The last buffer of this part has now been included into chains
                        for ( int slot : slots )
                        {
                            chainHeader.mark( slot, false );
                        }
                    }
                    if ( intermediateBuffer.isSplit() )
                    {
                        for ( int slot : slots )
                        {
                            linkHeader.markReference( slot, true );
                        }
                    }
                }
                else
                {
                    // We couldn't fit this buffer, so revert the tentative mark of the link header
                    for ( int slot : slots )
                    {
                        linkHeader.mark( slot, false );
                    }
                }
            }
            return spaceLeftInXL;
        }

        private static void serializeParts( ByteBuffer into, IntermediateBuffer[] intermediateBuffers, ByteBuffer recordPointersBuffer, Header header )
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        {
            header.allocateSpace( into );
            if ( header.hasMark( Header.FLAG_LABELS ) )
            {
<<<<<<< HEAD
                into.put( intermediateBuffers[LABELS] );
            }
=======
                into.put( intermediateBuffers[LABELS].get() );
                intermediateBuffers[LABELS].prev();
            }
            if ( header.hasMark( Header.OFFSET_RECORD_POINTER ) )
            {
                header.setOffset( Header.OFFSET_RECORD_POINTER, into.position() );
                into.put( recordPointersBuffer );
            }

>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            serializePart( into, intermediateBuffers[PROPERTIES], header, Header.OFFSET_PROPERTIES );
            serializePart( into, intermediateBuffers[RELATIONSHIPS], header, Header.OFFSET_RELATIONSHIPS );
            serializePart( into, intermediateBuffers[RELATIONSHIPS_OFFSETS], header, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
            serializePart( into, intermediateBuffers[DEGREES], header, Header.OFFSET_DEGREES );
<<<<<<< HEAD
            serializePart( into, intermediateBuffers[RECORD_POINTER], header, Header.OFFSET_RECORD_POINTER );
            serializePart( into, intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID], header, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            int endPosition = into.position();
            header.serialize( into.position( 0 ), referenceHeader );
            into.position( endPosition ).flip();
        }

        private static void serializePart( ByteBuffer into, ByteBuffer part, Header header, int slot )
=======
            serializePart( into, intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID], header, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            int endPosition = into.position();
            header.setOffset( Header.OFFSET_END, endPosition );
            header.serialize( into.position( 0 ) );
            into.position( endPosition ).flip();
        }

        private static void serializePart( ByteBuffer into, IntermediateBuffer part, Header header, int slot )
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        {
            if ( header.hasMark( slot ) )
            {
                header.setOffset( slot, into.position() );
<<<<<<< HEAD
                into.put( part );
            }
        }

        private Record deletedVersionOf( Record record )
        {
            Record deletedRecord = new Record( record.sizeExp(), record.id );
            deletedRecord.setFlag( FLAG_IN_USE, false );
            return deletedRecord;
        }

=======
                into.put( part.get() );
                part.prev();
            }
        }

>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        private void moveDataToDense()
        {
            if ( dense == null )
            {
                dense = new DenseRecordAndData( sparse, stores.denseStore, stores.bigPropertyValueStore, bigValueCommandConsumer, cursorTracer );
            }
            dense.moveDataFrom( sparse );
        }

        /**
         * @return the existing or allocated xL ID into sparse.data so that X1 can be serialized afterwards
         */
<<<<<<< HEAD
        private long xLargeCommands( ByteBuffer maxBuffer, SimpleStore store, Consumer<StorageCommand> commands )
        {
            Record after;
            int sizeExp = store.recordSizeExponential();
            if ( xLBefore != null && xLBefore.sizeExp() == sizeExp )
            {
                // There was a large record before and we're just modifying it
                commands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, after = recordForData( xLBefore.id, maxBuffer, sizeExp ) ) );
=======
        private long addXLRecords( ByteBuffer maxBuffer, SimpleStore store, FrekiCommand.SparseNode command )
        {
            Record after;
            int sizeExp = store.recordSizeExponential();
            Record xLBefore = takeBeforeRecord( s -> s == sizeExp );
            if ( xLBefore != null && xLBefore.sizeExp() == sizeExp )
            {
                // There was a large record before and we're just modifying it
                after = recordForData( xLBefore.id, maxBuffer, sizeExp );
                if ( contentsDiffer( xLBefore, after ) )
                {
                    command.addChange( xLBefore, after );
                }
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            }
            else if ( xLBefore != null && xLBefore.sizeExp() != sizeExp )
            {
                // There was a large record before, but this time it'll be of a different size, so a different one
<<<<<<< HEAD
                commands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, deletedVersionOf( xLBefore ) ) );
                long recordId = store.nextId( cursorTracer );
                commands.accept(
                        new FrekiCommand.SparseNode( nodeId, new Record( sizeExp, recordId ), after = recordForData( recordId, maxBuffer, sizeExp ) ) );
=======
                command.addChange( xLBefore, null );
                long recordId = store.nextId( cursorTracer );
                command.addChange( null, after = recordForData( recordId, maxBuffer, sizeExp ) );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            }
            else
            {
                // There was no large record before at all
                long recordId = store.nextId( cursorTracer );
<<<<<<< HEAD
                commands.accept(
                        new FrekiCommand.SparseNode( nodeId, new Record( sizeExp, recordId ), after = recordForData( recordId, maxBuffer, sizeExp ) ) );
=======
                command.addChange( null, after = recordForData( recordId, maxBuffer, sizeExp ) );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            }

            return buildRecordPointer( after.sizeExp(), after.id );
        }

<<<<<<< HEAD
        private void x1Command( ByteBuffer smallBuffer, Consumer<StorageCommand> commands )
        {
            Record before = x1Before != null ? x1Before : new Record( 0, nodeId );
            Record after = recordForData( nodeId, smallBuffer, 0 );
            commands.accept( new FrekiCommand.SparseNode( nodeId, before, after ) );
=======
        private void addX1Record( ByteBuffer smallBuffer, FrekiCommand.SparseNode node )
        {
            Record after = recordForData( nodeId, smallBuffer, 0 );
            Record before = takeBeforeRecord( sizeExp -> sizeExp == 0 );
            if ( before == null || contentsDiffer( before, after ) )
            {
                node.addChange( before, after );
            }
        }

        private Record takeBeforeRecord( IntPredicate filter )
        {
            //since we serialize XL first and chain backwards, its important beforeRecord
            RecordChain prev = null;
            for ( RecordChain chain = firstBeforeRecord; chain != null; prev = chain, chain = chain.next )
            {
                if ( filter.test( chain.record.sizeExp() ) )
                {
                    if ( prev == null )
                    {
                        boolean firstAndLastIsSame = firstBeforeRecord == lastBeforeRecord;
                        firstBeforeRecord = chain.next;
                        if ( firstAndLastIsSame )
                        {
                            lastBeforeRecord = firstBeforeRecord;
                        }
                    }
                    else
                    {
                        prev.next = chain.next;
                    }
                    return chain.record;
                }
            }
            return null;
        }

        private boolean contentsDiffer( Record before, Record after )
        {
            if ( before.data().limit() != after.data().limit() )
            {
                return true;
            }
            return !Arrays.equals(
                    before.data().array(), 0, before.data().limit(),
                    after.data().array(), 0, after.data().limit() );
        }
    }

    private static class RecordChain
    {
        private final Record record;
        private RecordChain next;

        RecordChain( Record record )
        {
            this.record = record;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        }
    }

    private static class SparseRecordAndData extends NodeDataModifier
    {
<<<<<<< HEAD
        private List<MutableNodeData> datas;
        private boolean deleted;
        private long nodeId;
        private final MainStores stores;
        private final PageCursorTracer cursorTracer;
=======
        private MutableNodeData data;
        private boolean deleted;
        private long nodeId;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec

        SparseRecordAndData( long nodeId, MainStores stores, PageCursorTracer cursorTracer )
        {
            this.nodeId = nodeId;
<<<<<<< HEAD
            this.stores = stores;
            this.cursorTracer = cursorTracer;
            this.datas = new ArrayList<>();
=======
            data = new MutableNodeData( nodeId, stores.bigPropertyValueStore, cursorTracer );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        }

        MutableNodeData add( Record record )
        {
<<<<<<< HEAD
            MutableNodeData data = new MutableNodeData( nodeId, stores.bigPropertyValueStore, cursorTracer, record.data() );
            datas.add( data );
            return data;
        }

        void addEmpty()
        {
            datas.add( new MutableNodeData( nodeId, stores.bigPropertyValueStore, cursorTracer ) );
        }

        private MutableNodeData dataFor( int headerMark )
        {
            MutableNodeData first = datas.get( 0 );
            if ( first.hasHeaderMark( headerMark ) || !first.hasHeaderReferenceMark( headerMark ) )
            {
                return first;
            }
            for ( int i = 1; i < datas.size(); i++ )
            {
                MutableNodeData data = datas.get( i );
                if ( data.hasHeaderMark( headerMark ) )
                {
                    return data;
                }
            }
            throw new UnsupportedOperationException( "X1 had reference marker for " + headerMark + ", but none had it" );
        }

        @Override
        public void updateLabels( LongSet added, LongSet removed )
        {
            MutableNodeData data = dataFor( Header.FLAG_LABELS );
=======
            data.deserialize( record );
            return data;
        }

        @Override
        public void updateLabels( LongSet added, LongSet removed )
        {
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            added.forEach( label -> data.addLabel( toIntExact( label ) ) );
            removed.forEach( label -> data.removeLabel( toIntExact( label ) ) );
        }

        @Override
        public void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties )
        {
<<<<<<< HEAD
            MutableNodeData.Relationship relationship = dataFor( Header.OFFSET_RELATIONSHIPS ).createRelationship( internalId, targetNode, type, outgoing );
=======
            MutableNodeData.Relationship relationship = data.createRelationship( internalId, targetNode, type, outgoing );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            for ( StorageProperty property : properties )
            {
                relationship.addProperty( property.propertyKeyId(), property.value() );
            }
        }

        @Override
        public void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
<<<<<<< HEAD
            MutableNodeData data = dataFor( Header.OFFSET_PROPERTIES );
=======
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            added.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            changed.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            removed.forEach( data::removeNodeProperty );
        }

        @Override
        public void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
<<<<<<< HEAD
            dataFor( Header.OFFSET_RELATIONSHIPS ).deleteRelationship( internalId, type, otherNode, outgoing );
=======
            data.deleteRelationship( internalId, type, otherNode, outgoing );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        }

        @Override
        void updateRelationshipProperties( long internalId, int type, long otherNode, boolean outgoing, Iterable<StorageProperty> added,
                Iterable<StorageProperty> changed, IntIterable removed )
        {
<<<<<<< HEAD
            dataFor( Header.OFFSET_RELATIONSHIPS ).updateRelationshipProperties( internalId, type, nodeId, otherNode, outgoing, added, changed, removed );
=======
            data.updateRelationshipProperties( internalId, type, nodeId, otherNode, outgoing, added, changed, removed );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        }

        @Override
        public void delete()
        {
            this.deleted = true;
        }

        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            // Sanity-check so that, if this node has been deleted it cannot have any relationships left in it
            if ( deleted )
            {
<<<<<<< HEAD
                MutableNodeData data = dataFor( Header.OFFSET_RELATIONSHIPS );
=======
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
                if ( data.hasRelationships() )
                {
                    throw new DeletedNodeStillHasRelationships( data.nodeId );
                }
            }
        }
    }

    private static class DenseRecordAndData extends NodeDataModifier
    {
        // meta
        private final SparseRecordAndData sparse;
        private final DenseRelationshipStore store;
        private final SimpleBigValueStore bigValueStore;
        private final Consumer<StorageCommand> bigValueConsumer;
        private final PageCursorTracer cursorTracer;
        private boolean deleted;

        // changes
        // TODO it feels like we've simply moving tx-state data from one form to another and that's probably true and can probably be improved on later
<<<<<<< HEAD
        private MutableIntObjectMap<DenseRelationships> relationshipUpdates = IntObjectMaps.mutable.empty();
=======
        private final MutableIntObjectMap<DenseRelationships> relationshipUpdates = IntObjectMaps.mutable.empty();
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec

        DenseRecordAndData( SparseRecordAndData sparse, DenseRelationshipStore store, SimpleBigValueStore bigValueStore,
                Consumer<StorageCommand> bigValueConsumer, PageCursorTracer cursorTracer )
        {
            this.sparse = sparse;
            this.store = store;
            this.bigValueStore = bigValueStore;
            this.bigValueConsumer = bigValueConsumer;
            this.cursorTracer = cursorTracer;
        }

        private long nodeId()
        {
            return sparse.nodeId;
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
<<<<<<< HEAD
            sparse.dataFor( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ).registerInternalRelationshipId( internalId );
            sparse.dataFor( Header.OFFSET_DEGREES ).addDegree( type, calculateDirection( targetNode, outgoing ), 1 );
=======
            sparse.data.registerInternalRelationshipId( internalId );
            sparse.data.addDegree( type, calculateDirection( targetNode, outgoing ), 1 );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            relationshipUpdatesForType( type ).insert( new DenseRelationships.DenseRelationship( internalId, targetNode, outgoing, properties ) );
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
<<<<<<< HEAD
            sparse.dataFor( Header.OFFSET_DEGREES ).addDegree( type, calculateDirection( otherNode, outgoing ), -1 );
=======
            sparse.data.addDegree( type, calculateDirection( otherNode, outgoing ), -1 );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
            relationshipUpdatesForType( type ).delete( new DenseRelationships.DenseRelationship( internalId, otherNode, outgoing,
                    store.loadRelationshipProperties( nodeId(), internalId, type, otherNode, outgoing, PropertyUpdate::remove, cursorTracer ) ) );
        }

        @Override
        public void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        void updateRelationshipProperties( long internalId, int type, long otherNode, boolean outgoing, Iterable<StorageProperty> added,
                Iterable<StorageProperty> changedIterable, IntIterable removedIterable )
        {
            MutableIntObjectMap<PropertyUpdate> properties =
                    store.loadRelationshipProperties( nodeId(), internalId, type, otherNode, outgoing, PropertyUpdate::add, cursorTracer );
            for ( StorageProperty property : added )
            {
                int key = property.propertyKeyId();
                properties.put( key, PropertyUpdate.add( key, serializeValue( bigValueStore, property.value(), bigValueConsumer ) ) );
            }
            Iterator<StorageProperty> changed = changedIterable.iterator();
            IntIterator removed = removedIterable.intIterator();
            if ( changed.hasNext() || removed.hasNext() )
            {
                while ( changed.hasNext() )
                {
                    StorageProperty property = changed.next();
                    int key = property.propertyKeyId();
                    PropertyUpdate existing = properties.get( key );
                    properties.put( key, PropertyUpdate.change( key, existing.after, serializeValue( bigValueStore, property.value(), bigValueConsumer ) ) );
                }
                while ( removed.hasNext() )
                {
                    int key = removed.next();
                    PropertyUpdate existing = properties.get( key );
                    properties.put( key, PropertyUpdate.remove( key, existing.after ) );
                }
            }
            relationshipUpdatesForType( type ).insert( new DenseRelationships.DenseRelationship( internalId, otherNode, outgoing, properties ) );
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
<<<<<<< HEAD
                if ( sparse.dataFor( Header.OFFSET_DEGREES ).hasAnyDegrees() )
=======
                if ( sparse.data.hasAnyDegrees() )
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
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
        void moveDataFrom( SparseRecordAndData sparseData )
        {
            // We're moving to the dense store, which from its POV all relationships will be created so therefore
            // start at 0 degrees and all creations will increment those degrees.
<<<<<<< HEAD
            sparseData.dataFor( Header.OFFSET_RELATIONSHIPS ).visitRelationships( ( type, fromRelationships ) -> fromRelationships.relationships.forEach(
                    from -> createRelationship( from.internalId, from.otherNode, from.type, from.outgoing,
                            serializeAddedProperties( from.properties, IntObjectMaps.mutable.empty() ) ) ) );
            MutableNodeData nextRelationshipIdData = sparseData.dataFor( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            nextRelationshipIdData.setNextInternalRelationshipId( nextRelationshipIdData.getNextInternalRelationshipId() );
            MutableNodeData relationshipsData = sparseData.dataFor( Header.OFFSET_RELATIONSHIPS );
            relationshipsData.clearRelationships();
            sparseData.datas.forEach( data -> data.setDense( true ) );
=======
            sparseData.data.visitRelationships( ( type, fromRelationships ) -> fromRelationships.relationships.forEach(
                    from -> createRelationship( from.internalId, from.otherNode, from.type, from.outgoing,
                            serializeAddedProperties( from.properties, IntObjectMaps.mutable.empty() ) ) ) );
            MutableNodeData nextRelationshipIdData = sparseData.data;
            nextRelationshipIdData.setNextInternalRelationshipId( nextRelationshipIdData.getNextInternalRelationshipId() );
            MutableNodeData relationshipsData = sparseData.data;
            relationshipsData.clearRelationships();
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
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
<<<<<<< HEAD
=======
        byteBuffer.position( buffer.limit() ).flip();
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
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
