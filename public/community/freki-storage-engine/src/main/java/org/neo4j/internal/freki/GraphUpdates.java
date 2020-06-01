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
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationships;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeData.buildRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.recordPointerToString;
import static org.neo4j.internal.freki.MutableNodeData.serializeRecordPointers;
import static org.neo4j.internal.freki.MutableNodeData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.DUAL_VLONG_MAX_SIZE;
import static org.neo4j.internal.freki.StreamVByte.SINGLE_VLONG_MAX_SIZE;

/**
 * Contains all logic about making graph data changes to a Freki store, everything from loading and modifying data to serializing
 * and overflowing into larger records or dense store.
 */
class GraphUpdates
{
    private static final int WORST_CASE_HEADER_AND_STUFF_SIZE = Header.WORST_CASE_SIZE + DUAL_VLONG_MAX_SIZE;

    private final Collection<FrekiCommand.BigPropertyValue> bigValueCreationCommands = new ArrayList<>();
    private final Collection<FrekiCommand.BigPropertyValue> bigValueDeletionCommands = new ArrayList<>();
    private final Collection<FrekiCommand.DenseNode> denseCommands = new ArrayList<>();
    private final MemoryTracker memoryTracker;
    private final TreeMap<Long,NodeUpdates> mutations = new TreeMap<>();
    private final MainStores stores;
    private final PageCursorTracer cursorTracer;

    //Intermediate buffer slots
    static final int PROPERTIES = 0;
    static final int RELATIONSHIPS = 1;
    static final int DEGREES = 2;
    static final int RELATIONSHIPS_OFFSETS = 3;
    static final int NEXT_INTERNAL_RELATIONSHIP_ID = 4;
    static final int LABELS = 5;
    static final int NUM_BUFFERS = LABELS + 1;

    GraphUpdates( MainStores stores, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this.stores = stores;
        this.cursorTracer = cursorTracer;
        this.memoryTracker = memoryTracker;
    }

    NodeUpdates getOrLoad( long nodeId )
    {
        return mutations.computeIfAbsent( nodeId, id ->
        {
            NodeUpdates updates = new NodeUpdates( id, stores, bigValueCreationCommands::add, bigValueDeletionCommands::add, cursorTracer );
            updates.load();
            return updates;
        } );
    }

    void create( long nodeId )
    {
        NodeUpdates updates = new NodeUpdates( nodeId, stores, bigValueCreationCommands::add, bigValueDeletionCommands::add, cursorTracer );
        mutations.put( nodeId, updates );
    }

    void extractUpdates( Consumer<StorageCommand> commands ) throws ConstraintViolationTransactionFailureException
    {
        List<StorageCommand> otherCommands = new ArrayList<>();
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

        ByteBuffer smallBuffer = ByteBuffer.wrap( new byte[stores.mainStore.recordDataSize()] );
        ByteBuffer maxBuffer = ByteBuffer.wrap( new byte[x8Size] );
        Header x1Header = new Header();
        Header xLHeader = new Header();
        for ( NodeUpdates mutation : mutations.values() )
        {
            mutation.serialize( smallBuffer, maxBuffer, intermediateBuffers, recordPointersBuffer, denseCommands::add, otherCommands::add, x1Header, xLHeader );
        }
        denseCommands.forEach( commands );
        bigValueCreationCommands.forEach( commands );
        otherCommands.forEach( commands );
        bigValueDeletionCommands.forEach( commands );
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
        private final Consumer<FrekiCommand.BigPropertyValue> bigValueCreations;
        private final Consumer<FrekiCommand.BigPropertyValue> bigValueDeletions;
        private final PageCursorTracer cursorTracer;

        // the before-state
        private RecordChain firstBeforeRecord;
        private RecordChain lastBeforeRecord;
        private byte version = Record.UNVERSIONED;

        // the after-state
        private final SparseRecordAndData sparse;
        private DenseRecordAndData dense;
        private boolean deleted;
        private MutableLongList deletedBigValueIds;
        private byte nextVersion = nextVersion( version );

        NodeUpdates( long nodeId, MainStores stores, Consumer<FrekiCommand.BigPropertyValue> bigValueCreations,
                Consumer<FrekiCommand.BigPropertyValue> bigValueDeletions, PageCursorTracer cursorTracer )
        {
            this.nodeId = nodeId;
            this.stores = stores;
            this.bigValueCreations = bigValueCreations;
            this.bigValueDeletions = bigValueDeletions;
            this.cursorTracer = cursorTracer;
            this.sparse = new SparseRecordAndData( nodeId, stores, this::keepTrackOfDeletedBigValueIds, cursorTracer );
        }

        private byte nextVersion( byte version )
        {
            do
            {
                version = (byte) (version + 1);
            }
            while ( version == Record.UNVERSIONED );
            return version;
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
            version = x1.version;
            nextVersion = nextVersion( x1.version );

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
                if ( version != xL.version )
                {
                    throw new IllegalStateException( String.format( "Node[%s,%s] has mismatching versions in chain. sizeExp:%s, id:%s version:%s ", nodeId,
                            version, xL.id, xL.sizeExp(), xL.version ) );
                }
                sparse.add( xL );
                RecordChain chain = new RecordChain( xL );
                chain.next = firstBeforeRecord;
                firstBeforeRecord = chain;
            }
            if ( data.isDense() )
            {
                dense = new DenseRecordAndData( sparse, stores.denseStore, stores.bigPropertyValueStore, bigValueCreations,
                        this::keepTrackOfDeletedBigValueIds, cursorTracer );
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

        private void keepTrackOfDeletedBigValueIds( Value removedValue )
        {
            assert removedValue != null;
            if ( removedValue instanceof PropertyValueFormat.PointerValue )
            {
                if ( deletedBigValueIds == null )
                {
                    deletedBigValueIds = LongLists.mutable.empty();
                }
                deletedBigValueIds.add( ((PropertyValueFormat.PointerValue) removedValue).pointer() );
            }
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
            // Reset the positions of the before buffers
            // assumption: MutableNodeData has already looked at the header and set the limit to the end of the effective data
            for ( RecordChain chain = firstBeforeRecord; chain != null; chain = chain.next )
            {
                chain.record.data( 0 );
            }
        }

        void serialize( ByteBuffer smallBuffer, ByteBuffer maxBuffer, IntermediateBuffer[] intermediateBuffers, ByteBuffer recordPointersBuffer,
                Consumer<FrekiCommand.DenseNode> denseCommands, Consumer<StorageCommand> otherCommands,
                Header x1Header, Header xLHeader ) throws ConstraintViolationTransactionFailureException
        {
            prepareForCommandExtraction();
            addDeletedBigValueCommands();

            if ( deleted )
            {
                deletionCommands( otherCommands );
                return;
            }

            smallBuffer.clear();
            maxBuffer.clear();
            for ( IntermediateBuffer buffer : intermediateBuffers )
            {
                buffer.clear();
            }

            boolean isDense = sparse.data.serializeMainData( intermediateBuffers, stores.bigPropertyValueStore, bigValueCreations );
            intermediateBuffers[LABELS].flip();
            intermediateBuffers[PROPERTIES].flip();
            intermediateBuffers[RELATIONSHIPS].flip();
            intermediateBuffers[RELATIONSHIPS_OFFSETS].flip();
            intermediateBuffers[DEGREES].flip();
            int relsSize = intermediateBuffers[RELATIONSHIPS].currentSize() + intermediateBuffers[RELATIONSHIPS_OFFSETS].currentSize();
            isDense |= relsSize > intermediateBuffers[RELATIONSHIPS].capacity();
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
                }
                dense.createCommands( denseCommands );
            }

            // X LEGO TIME
            int miscSize = 0;
            x1Header.clearMarks();
            x1Header.mark( Header.OFFSET_END, true );
            x1Header.mark( Header.FLAG_LABELS, intermediateBuffers[LABELS].currentSize() > 0 );
            x1Header.mark( Header.OFFSET_PROPERTIES, intermediateBuffers[PROPERTIES].currentSize() > 0 );
            if ( isDense )
            {
                x1Header.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, true );
                x1Header.mark( Header.OFFSET_DEGREES, true );
                x1Header.mark( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID, true );
                sparse.data.serializeNextInternalRelationshipId( intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].add() );
                intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].flip();
                miscSize += intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].currentSize();
            }
            else if ( relsSize > 0 )
            {
                x1Header.mark( Header.OFFSET_RELATIONSHIPS, true );
                x1Header.mark( Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS, true );
            }

            boolean anyBufferIsSplit = false;
            for ( int i = 0; i < intermediateBuffers.length && !anyBufferIsSplit; i++ )
            {
                anyBufferIsSplit = intermediateBuffers[i].isSplit();
            }

            FrekiCommand.SparseNode command = new FrekiCommand.SparseNode( nodeId );
            if ( !anyBufferIsSplit && x1Header.spaceNeeded() + intermediateBuffers[LABELS].currentSize() + intermediateBuffers[PROPERTIES].currentSize() +
                    Math.max( relsSize, intermediateBuffers[DEGREES].currentSize() ) + miscSize <= stores.mainStore.recordDataSize() )
            {
                //WE FIT IN x1
                serializeParts( smallBuffer, intermediateBuffers, recordPointersBuffer, x1Header );
                addX1Record( smallBuffer, command );
                deleteRemainingBeforeRecords( command );
                otherCommands.accept( command );
                return;
            }

            //we did not fit in x1 only, fit as many things as possible in x1
            int worstCaseMiscSize = miscSize + SINGLE_VLONG_MAX_SIZE;

            x1Header.clearMarks();
            // build x1 header
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
            boolean canFitInSingleXL = !anyBufferIsSplit &&
                    (xLHeader.hasMark( Header.FLAG_LABELS ) ? intermediateBuffers[LABELS].currentSize() : 0) +
                    (xLHeader.hasMark( Header.OFFSET_PROPERTIES ) ? intermediateBuffers[PROPERTIES].currentSize() : 0) +
                    (xLHeader.hasMark( Header.OFFSET_RELATIONSHIPS ) ? relsSize : 0) +
                    (xLHeader.hasMark( Header.OFFSET_DEGREES ) ? intermediateBuffers[DEGREES].currentSize() : 0) +
                    (xLHeader.hasMark( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ) ?
                        intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].currentSize() : 0) + xLHeader.spaceNeeded() +
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

        private void addDeletedBigValueCommands()
        {
            if ( deletedBigValueIds != null )
            {
                try ( PageCursor cursor = stores.bigPropertyValueStore.openReadCursor( cursorTracer ) )
                {
                    deletedBigValueIds.forEach( deletedBigValueId ->
                    {
                        List<Record> records = new ArrayList<>();
                        stores.bigPropertyValueStore.visitRecordChainIds( cursor, deletedBigValueId,
                                recordId -> records.add( Record.deletedRecord( (byte) 0, recordId ) ) );
                        bigValueDeletions.accept( new FrekiCommand.BigPropertyValue( records ) );
                    } );
                }
            }
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
        {
            header.allocateSpace( into );
            if ( header.hasMark( Header.FLAG_LABELS ) )
            {
                into.put( intermediateBuffers[LABELS].get() );
                intermediateBuffers[LABELS].prev();
            }
            if ( header.hasMark( Header.OFFSET_RECORD_POINTER ) )
            {
                header.setOffset( Header.OFFSET_RECORD_POINTER, into.position() );
                into.put( recordPointersBuffer );
            }

            serializePart( into, intermediateBuffers[PROPERTIES], header, Header.OFFSET_PROPERTIES );
            serializePart( into, intermediateBuffers[RELATIONSHIPS], header, Header.OFFSET_RELATIONSHIPS );
            serializePart( into, intermediateBuffers[RELATIONSHIPS_OFFSETS], header, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
            serializePart( into, intermediateBuffers[DEGREES], header, Header.OFFSET_DEGREES );
            serializePart( into, intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID], header, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
            int endPosition = into.position();
            header.setOffset( Header.OFFSET_END, endPosition );
            header.serialize( into.position( 0 ) );
            into.position( endPosition ).flip();
        }

        private static void serializePart( ByteBuffer into, IntermediateBuffer part, Header header, int slot )
        {
            if ( header.hasMark( slot ) )
            {
                header.setOffset( slot, into.position() );
                into.put( part.get() );
                part.prev();
            }
        }

        private void moveDataToDense()
        {
            if ( dense == null )
            {
                dense = new DenseRecordAndData( sparse, stores.denseStore, stores.bigPropertyValueStore, bigValueCreations,
                        this::keepTrackOfDeletedBigValueIds, cursorTracer );
            }
            dense.moveDataFrom( sparse );
        }

        /**
         * @return the existing or allocated xL ID into sparse.data so that X1 can be serialized afterwards
         */
        private long addXLRecords( ByteBuffer maxBuffer, SimpleStore store, FrekiCommand.SparseNode command )
        {
            Record after;
            int sizeExp = store.recordSizeExponential();
            Record xLBefore = takeBeforeRecord( s -> s == sizeExp );
            if ( xLBefore != null && xLBefore.sizeExp() == sizeExp )
            {
                // There was a large record before and we're just modifying it
                after = recordForData( xLBefore.id, maxBuffer, sizeExp, nextVersion );
                if ( contentsDiffer( xLBefore, after ) )
                {
                    command.addChange( xLBefore, after );
                }
            }
            else if ( xLBefore != null && xLBefore.sizeExp() != sizeExp )
            {
                // There was a large record before, but this time it'll be of a different size, so a different one
                command.addChange( xLBefore, null );
                long recordId = store.nextId( cursorTracer );
                command.addChange( null, after = recordForData( recordId, maxBuffer, sizeExp, nextVersion ) );
            }
            else
            {
                // There was no large record before at all
                long recordId = store.nextId( cursorTracer );
                command.addChange( null, after = recordForData( recordId, maxBuffer, sizeExp, nextVersion ) );
            }

            return buildRecordPointer( after.sizeExp(), after.id );
        }

        private void addX1Record( ByteBuffer smallBuffer, FrekiCommand.SparseNode node )
        {
            Record after = recordForData( nodeId, smallBuffer, 0, nextVersion );
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
            return true; //TODO what do we do when we dont really wanna update the record but version needs to be updated anyway?
//            if ( before.data().limit() != after.data().limit() )
//            {
//                return true;
//            }
//            return !Arrays.equals(
//                    before.data().array(), 0, before.data().limit(),
//                    after.data().array(), 0, after.data().limit() );
        }
    }

    private static class RecordChain
    {
        private final Record record;
        private RecordChain next;

        RecordChain( Record record )
        {
            this.record = record;
        }
    }

    private static class SparseRecordAndData extends NodeDataModifier
    {
        private final MutableNodeData data;
        private final long nodeId;
        private final Consumer<Value> removedValuesBin;
        private boolean deleted;

        SparseRecordAndData( long nodeId, MainStores stores, Consumer<Value> removedValuesBin, PageCursorTracer cursorTracer )
        {
            this.nodeId = nodeId;
            this.removedValuesBin = removedValuesBin;
            this.data = new MutableNodeData( nodeId, stores.bigPropertyValueStore, cursorTracer );
        }

        MutableNodeData add( Record record )
        {
            data.deserialize( record );
            return data;
        }

        @Override
        public void updateLabels( LongSet added, LongSet removed )
        {
            added.forEach( label -> data.addLabel( toIntExact( label ) ) );
            removed.forEach( label -> data.removeLabel( toIntExact( label ) ) );
        }

        @Override
        public void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties )
        {
            MutableNodeData.Relationship relationship = data.createRelationship( internalId, targetNode, type, outgoing );
            for ( StorageProperty property : properties )
            {
                relationship.addProperty( property.propertyKeyId(), property.value() );
            }
        }

        @Override
        public void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            added.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            changed.forEach( p -> removedValuesBin.accept( data.setNodeProperty( p.propertyKeyId(), p.value() ) ) );
            removed.forEach( key -> removedValuesBin.accept( data.removeNodeProperty( key ) ) );
        }

        @Override
        public void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            data.deleteRelationship( internalId, type, otherNode, outgoing, removedValuesBin );
        }

        @Override
        void updateRelationshipProperties( long internalId, int type, long otherNode, boolean outgoing, Iterable<StorageProperty> added,
                Iterable<StorageProperty> changed, IntIterable removed )
        {
            data.updateRelationshipProperties( internalId, type, nodeId, otherNode, outgoing, added, changed, removed, removedValuesBin );
        }

        @Override
        public void delete()
        {
            deleted = true;
            data.visitNodePropertyValues( removedValuesBin );
        }

        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            // Sanity-check so that, if this node has been deleted it cannot have any relationships left in it
            if ( deleted )
            {
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
        private final SimpleDenseRelationshipStore store;
        private final SimpleBigValueStore bigValueStore;
        private final Consumer<FrekiCommand.BigPropertyValue> createBigValues;
        private final Consumer<Value> removedValuesBin;
        private final PageCursorTracer cursorTracer;
        private boolean deleted;

        // changes
        // TODO it feels like we've simply moving tx-state data from one form to another and that's probably true and can probably be improved on later
        private final TreeMap<Integer,DenseRelationships> relationshipUpdates = new TreeMap<>();

        DenseRecordAndData( SparseRecordAndData sparse, SimpleDenseRelationshipStore store, SimpleBigValueStore bigValueStore,
                Consumer<FrekiCommand.BigPropertyValue> createdBigValues, Consumer<Value> removedValuesBin, PageCursorTracer cursorTracer )
        {
            this.sparse = sparse;
            this.store = store;
            this.bigValueStore = bigValueStore;
            this.createBigValues = createdBigValues;
            this.removedValuesBin = removedValuesBin;
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
            sparse.data.registerInternalRelationshipId( internalId );
            sparse.data.addDegree( type, calculateDirection( targetNode, outgoing ), 1 );
            relationshipUpdatesForType( type ).add( new DenseRelationships.DenseRelationship( internalId, targetNode, outgoing, properties, false ) );
        }

        private RelationshipDirection calculateDirection( long targetNode, boolean outgoing )
        {
            return nodeId() == targetNode ? RelationshipDirection.LOOP : outgoing ? RelationshipDirection.OUTGOING : RelationshipDirection.INCOMING;
        }

        private DenseRelationships relationshipUpdatesForType( int type )
        {
            return relationshipUpdates.computeIfAbsent( type, t -> new DenseRelationships( nodeId(), t ) );
        }

        @Override
        public void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            // TODO have some way of at least saying whether or not this relationship had properties, so that this loading can be skipped completely
            sparse.data.addDegree( type, calculateDirection( otherNode, outgoing ), -1 );
            MutableIntObjectMap<PropertyUpdate> properties =
                    store.loadRelationshipProperties( nodeId(), internalId, type, otherNode, outgoing, PropertyUpdate::remove, cursorTracer );
            relationshipUpdatesForType( type ).add( new DenseRelationships.DenseRelationship( internalId, otherNode, outgoing, properties, true ) );
            properties.forEachValue( update -> checkAddBigValueToRemovedValuesBin( update.before ) );
        }

        private void checkAddBigValueToRemovedValuesBin( ByteBuffer buffer )
        {
            if ( PropertyValueFormat.isPointerValue( buffer ) )
            {
                removedValuesBin.accept( PropertyValueFormat.read( buffer, bigValueStore, cursorTracer ) );
            }
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
                properties.put( key, PropertyUpdate.add( key, serializeValue( bigValueStore, property.value(), createBigValues ) ) );
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
                    properties.put( key, PropertyUpdate.change( key, existing.after, serializeValue( bigValueStore, property.value(), createBigValues ) ) );
                    checkAddBigValueToRemovedValuesBin( existing.after );
                }
                while ( removed.hasNext() )
                {
                    int key = removed.next();
                    PropertyUpdate existing = properties.get( key );
                    properties.put( key, PropertyUpdate.remove( key, existing.after ) );
                    checkAddBigValueToRemovedValuesBin( existing.after );
                }
            }
            relationshipUpdatesForType( type ).add( new DenseRelationships.DenseRelationship( internalId, otherNode, outgoing, properties, false ) );
        }

        @Override
        public void delete()
        {
            deleted = true;
        }

        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            // Be mechanically sympathetic to the applier by sorting the relationship updates
            for ( DenseRelationships relationships : relationshipUpdates.values() )
            {
                Collections.sort( relationships.relationships );
            }
            if ( deleted )
            {
                // This dense node has now been deleted, verify that all its relationships have also been removed in this transaction
                if ( sparse.data.hasAnyDegrees() )
                {
                    throw new DeletedNodeStillHasRelationships( nodeId() );
                }
            }
        }

        void createCommands( Consumer<FrekiCommand.DenseNode> commands )
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
            sparseData.data.visitRelationships( ( type, fromRelationships ) -> fromRelationships.relationships.forEach(
                    from -> createRelationship( from.internalId, from.otherNode, from.type, from.outgoing,
                            serializeAddedProperties( from.properties, IntObjectMaps.mutable.empty() ) ) ) );
            MutableNodeData nextRelationshipIdData = sparseData.data;
            nextRelationshipIdData.setNextInternalRelationshipId( nextRelationshipIdData.getNextInternalRelationshipId() );
            MutableNodeData relationshipsData = sparseData.data;
            relationshipsData.clearRelationships();
        }

        private IntObjectMap<PropertyUpdate> serializeAddedProperties( IntObjectMap<Value> properties, MutableIntObjectMap<PropertyUpdate> target )
        {
            properties.forEachKeyValue( ( key, value ) -> target.put( key, add( key, serializeValue( bigValueStore, value, createBigValues ) ) ) );
            return target;
        }

        private IntObjectMap<PropertyUpdate> serializeAddedProperties( Iterable<StorageProperty> properties, MutableIntObjectMap<PropertyUpdate> target )
        {
            properties.forEach( property -> target.put( property.propertyKeyId(),
                    add( property.propertyKeyId(), serializeValue( bigValueStore, property.value(), createBigValues ) ) ) );
            return target;
        }
    }

    private static Record recordForData( long recordId, ByteBuffer buffer, int sizeExp, byte newVersion )
    {
        Record after = new Record( sizeExp, recordId, newVersion );
        after.setFlag( FLAG_IN_USE, true );
        ByteBuffer byteBuffer = after.data();
        byteBuffer.put( buffer.array(), 0, buffer.limit() );
        byteBuffer.position( buffer.limit() ).flip();
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

    static ByteBuffer serializeValue( SimpleBigValueStore bigValueStore, Value value, Consumer<FrekiCommand.BigPropertyValue> bigValueCommandConsumer )
    {
        // TODO hand-wavy upper limit
        ByteBuffer buffer = ByteBuffer.wrap( new byte[256] );
        PropertyValueFormat format = new PropertyValueFormat( bigValueStore, bigValueCommandConsumer, buffer );
        value.writeTo( format );
        buffer.flip();
        return buffer;
    }
}
