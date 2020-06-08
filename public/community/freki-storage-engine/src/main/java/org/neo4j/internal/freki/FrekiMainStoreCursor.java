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

import java.nio.ByteBuffer;
import java.util.Arrays;
<<<<<<< HEAD

import org.neo4j.internal.freki.StreamVByte.IntArrayTarget;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
=======
import java.util.function.ToIntFunction;

import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.internal.freki.IntermediateBuffer.isFirstFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.isLastFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.ordinalFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.versionFromPieceHeader;
import static org.neo4j.internal.freki.MutableNodeData.forwardPointer;
import static org.neo4j.internal.freki.MutableNodeData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.readInts;
import static org.neo4j.internal.freki.StreamVByte.readLongs;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import static org.neo4j.util.Preconditions.checkState;

abstract class FrekiMainStoreCursor implements AutoCloseable
{
    static final long NULL = -1;
    static final int NULL_OFFSET = 0;
<<<<<<< HEAD
=======
    protected static final int MAX_RETRIES_BEFORE_TIMER = 10;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

    final MainStores stores;
    final CursorAccessPatternTracer cursorAccessPatternTracer;
    final CursorAccessPatternTracer.ThreadAccess cursorAccessTracer;
    final PageCursorTracer cursorTracer;
<<<<<<< HEAD
    boolean forceLoad;

    private Header header;
=======
    final MemoryTracker memoryTracker;
    boolean forceLoad;
    RecordLookup additionalRecordLookup;

>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    FrekiCursorData data;
    // State from relationship section, it's here because both relationship cursors as well as property cursor makes use of them
    int[] relationshipTypesInNode;
    int[] relationshipTypeOffsets;
    int firstRelationshipTypeOffset;

<<<<<<< HEAD
    private PageCursor[] xCursors;

    FrekiMainStoreCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer )
=======
    private final PageCursor[] xCursors;

    FrekiMainStoreCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    {
        this.stores = stores;
        this.cursorAccessPatternTracer = cursorAccessPatternTracer;
        this.cursorAccessTracer = cursorAccessPatternTracer.access();
        this.cursorTracer = cursorTracer;
        this.xCursors = new PageCursor[stores.getNumMainStores()];
<<<<<<< HEAD
=======
        this.memoryTracker = memoryTracker;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    public void reset()
    {
        if ( data != null )
        {
            resetOrDereferenceData();
        }
        relationshipTypeOffsets = null;
        relationshipTypesInNode = null;
        firstRelationshipTypeOffset = 0;
<<<<<<< HEAD
=======
        additionalRecordLookup = null;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    /**
     * @return {@code true} if data records were able to be reused, otherwise {@code false}.
     */
    public boolean resetOrDereferenceData()
    {
        if ( data.refCount > 1 )
        {
            dereferenceData();
            return false;
        }
        else if ( data.refCount == 1 )
        {
            data.reset();
        }
        return true;
    }

    void dereferenceData()
    {
        if ( data != null )
        {
            data.refCount--;
            data = null;
        }
    }

    PageCursor xCursor( int sizeExp )
    {
        if ( xCursors[sizeExp] == null )
        {
            xCursors[sizeExp] = stores.mainStore( sizeExp ).openReadCursor( cursorTracer );
        }
        return xCursors[sizeExp];
    }

    Record xRecord( int sizeExp )
    {
<<<<<<< HEAD
        if ( data.records[sizeExp] == null )
        {
            data.records[sizeExp] = stores.mainStore( sizeExp ).newRecord();
        }
        return data.records[sizeExp];
=======
        if ( data.recordIndex[sizeExp] < data.records[sizeExp].length - 1 )
        {
            data.recordIndex[sizeExp]++;
        }
        else
        {
            data.records[sizeExp][data.recordIndex[sizeExp]] = null;
        }

        if ( data.records[sizeExp][data.recordIndex[sizeExp]] == null )
        {
            data.records[sizeExp][data.recordIndex[sizeExp]] = stores.mainStore( sizeExp ).newRecord();
        }
        return data.records[sizeExp][data.recordIndex[sizeExp]];
    }

    boolean loadSuperLight( long nodeId )
    {
        boolean reused = ensureFreshDataInstanceForLoadingNewNode();
        cursorAccessTracer.registerNode( nodeId, reused );
        if ( !stores.mainStore.exists( xCursor( 0 ), nodeId ) )
        {
            return false;
        }
        data.nodeId = nodeId;
        return true;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    /**
     * Loads raw byte contents and does minimal parsing of higher-level offsets of the given node into {@link #data}.
     * @param nodeId node id to load.
     * @return {@code true} if the node exists, otherwise {@code false}.
     */
    boolean load( long nodeId )
    {
        boolean reused = ensureFreshDataInstanceForLoadingNewNode();
        cursorAccessTracer.registerNode( nodeId, reused );
        Record x1Record = loadRecord( 0, nodeId );
<<<<<<< HEAD
        if ( x1Record == null )
=======
        if ( x1Record == null || !data.gatherDataFromX1( x1Record ) )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        {
            return false;
        }

        data.nodeId = nodeId;
<<<<<<< HEAD
        data.gatherDataFromX1( x1Record );
        if ( data.forwardPointer == NULL )
        {
            data.xLLoaded = true;
        }
        // else xL can be loaded lazily when/if needed
        return true;
    }

    private void ensureXLLoaded()
    {
        if ( !data.xLLoaded && data.forwardPointer != NULL )
        {
            int sizeExp = sizeExponentialFromRecordPointer( data.forwardPointer );
            Record xLRecord = loadRecord( sizeExp, idFromRecordPointer( data.forwardPointer ) );
            if ( xLRecord != null )
            {
                data.gatherDataFromXL( xLRecord );
            }
            data.xLLoaded = true;
        }
    }

    void ensureRelationshipsLoaded()
    {
        if ( !data.isDense && data.relationshipOffset == 0 )
        {
            ensureXLLoaded();
        }
    }

    void ensurePropertiesLoaded()
    {
        if ( data.propertyOffset == 0 )
        {
            ensureXLLoaded();
        }
=======
        return true;
    }

    private boolean ensureX1Loaded()
    {
        return data.x1Loaded || load( data.nodeId );
    }

    //Package-private for testing/analysis purposes

    /**
     * @return {@code true} means the next chain link record was loaded correctly. {@code false} means either end of chain or that there was
     * a piece header mismatch, either way a retry is required.
     */
    boolean loadNextChainLink()
    {
        //This is only capable of reading chain in one direction, until reached end.
        if ( data.xLChainNextLinkPointer != NULL )
        {
            int sizeExp = sizeExponentialFromRecordPointer( data.xLChainNextLinkPointer );
            Record xLRecord = loadRecord( sizeExp, idFromRecordPointer( data.xLChainNextLinkPointer ) );
            return xLRecord != null && data.gatherDataFromXL( xLRecord );
            //Returning false means record has version mismatch or been deleted, likely reading while writing.
            //Caller is responsible for have to reload everything again.
        }
        // else: we reached end of chain
        return false;
    }

    enum SPLIT_PIECE_LOAD_STATUS
    {
        OK, ERR_UNUSED_RECORD, ERR_VERSION_MISMATCH, ERR_UNEXPECTED_ORDINAL, ERR_UNEXPECTED_FIRST, ERR_NO_LONGER_SLIT, ERR_NO_LONGER_EXISTS, ERR_END_NOT_FOUND
    }
    /**
     * Provides convenient way of iterating through data that is split into multiple records. The {@link FrekiCursorData} keeps a reference
     * to the first (in chain) occurrence of a certain data part, i.e. labels or properties. Then when actually reading the data of that part
     * this is the buffer to start from and to, when it runs out, advance to the next buffer containing more data for that part this
     * method is invoked to get there. Very convenient, but a bit wasteful since it loads records one more time and then throws them away.
     * This is done so that the book keeping for where data for various parts are is as slim as possible to not penalise the non-split case,
     * which btw is by far the most common one, so if this split-reading is slightly suboptimal then that's fine a.t.m.
     *
     * @param headerSlot which part to look for.
     * @param pieceLoadingState struct for holding state between calls for loading more pieces.
     * @return SPLIT_PIECE_LOAD_STATUS with OK or error code on failure
     */
    SPLIT_PIECE_LOAD_STATUS loadNextSplitPiece( int headerSlot, FrekiCursorData.PieceLoadingState pieceLoadingState )
    {
        ByteBuffer buffer = pieceLoadingState.buffer;
        Header header = pieceLoadingState.header;
        header.deserialize( buffer.position( 0 ) );
        long forwardPointer;
        while ( (forwardPointer = forwardPointer( readLongs( buffer.position( header.getOffset( Header.OFFSET_RECORD_POINTER ) ) ),
                buffer.limit() > stores.mainStore.recordDataSize() )) != NULL )
        {
            int sizeExp = sizeExponentialFromRecordPointer( forwardPointer );
            Record record = new Record( sizeExp );
            if ( !stores.mainStore( sizeExp ).read( xCursor( sizeExp ), record, idFromRecordPointer( forwardPointer ) ) || !record.hasFlag( FLAG_IN_USE ) )
            {
                return SPLIT_PIECE_LOAD_STATUS.ERR_UNUSED_RECORD;
            }
            buffer = record.data();
            pieceLoadingState.buffer = buffer;
            header.deserialize( buffer );
            if ( header.hasMark( headerSlot ) )
            {
                if ( header.hasReferenceMark( headerSlot ) )
                {
                    buffer.position( header.getOffset( headerSlot ) );
                    short pieceHeader = buffer.getShort();
                    byte partVersion = versionFromPieceHeader( pieceHeader );
                    byte pieceOrdinal = ordinalFromPieceHeader( pieceHeader );
                    pieceLoadingState.ordinal++;
                    pieceLoadingState.last = isLastFromPieceHeader( pieceHeader );

                    if ( pieceOrdinal != pieceLoadingState.ordinal )
                    {
                        return SPLIT_PIECE_LOAD_STATUS.ERR_UNEXPECTED_ORDINAL;
                    }
                    if ( isFirstFromPieceHeader( pieceHeader ) )
                    {
                        return SPLIT_PIECE_LOAD_STATUS.ERR_UNEXPECTED_FIRST;
                    }
                    if ( partVersion != pieceLoadingState.version )
                    {
                        return SPLIT_PIECE_LOAD_STATUS.ERR_VERSION_MISMATCH;
                    }
                    return SPLIT_PIECE_LOAD_STATUS.OK;
                }
                else
                {
                    return SPLIT_PIECE_LOAD_STATUS.ERR_NO_LONGER_SLIT;
                }
            }
            else if ( !header.hasReferenceMark( headerSlot ) )
            {
                return SPLIT_PIECE_LOAD_STATUS.ERR_NO_LONGER_EXISTS;
            }
            // else there could be another record in between the pieces for this data part, so just skip on through it
        }
        return SPLIT_PIECE_LOAD_STATUS.ERR_END_NOT_FOUND;
    }

    private boolean ensureLoaded( ToIntFunction<FrekiCursorData> test, int headerSlot )
    {
        if ( !ensureX1Loaded() )
        {
            return false;
        }
        while ( test.applyAsInt( data ) == 0 && data.header.hasReferenceMark( headerSlot ) )
        {
            if ( !loadNextChainLink() )
            {
                //We traversed the rest of the chain. A few possible scenarios
                //1. The header/store is corrupt / we did not start at the beginning of the chain.
                //2. Concurrent writes to this read and we moved the part (to earlier in the chain).
                //3. We failed to read next link, deleted or version mismatch
                //All we can do is retry, if its corruption we'll get retry timeout. If its concurrent read/writes we'll eventually succeed
                return false;
            }
        }
        return true;
    }

    private void ensureLoadedWithRetries( ToIntFunction<FrekiCursorData> test, int headerSlot )
    {
        for ( int i = 0; i < MAX_RETRIES_BEFORE_TIMER; i++ )
        {
            if ( ensureLoaded( test, headerSlot ) )
            {
                return;
            }
            prepareForReload();
        }

        //Double logic to avoid potential cost of System.currentTimeMillis() on hot-path.
        long end = System.currentTimeMillis() + 1000;
        do
        {
            if ( ensureLoaded( test, headerSlot ) )
            {
                return;
            }
            prepareForReload();
        }
        while ( System.currentTimeMillis() < end );
        throw new TransientTransactionFailureException(
                "Failed to load a consistent state of Node[" + data.nodeId + "]. Was modified too many times during read." );
    }

    void prepareForReload()
    {
        long id = data.nodeId;
        ensureFreshDataInstanceForLoadingNewNode();
        data.nodeId = id;
    }

    void ensureLabelsLocated()
    {
        ensureLoadedWithRetries( data -> data.labelOffset, Header.FLAG_LABELS );
    }

    void ensureRelationshipsLocated()
    {
        ensureLoadedWithRetries( data -> data.relationshipOffset, data.isDense ? Header.OFFSET_DEGREES : Header.OFFSET_RELATIONSHIPS );
    }

    void ensurePropertiesLocated()
    {
        ensureLoadedWithRetries( data -> data.propertyOffset, Header.OFFSET_PROPERTIES );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    private Record loadRecord( int sizeExp, long id )
    {
<<<<<<< HEAD
=======
        if ( additionalRecordLookup != null )
        {
            // Let's look in the additional lookup before poking the store
            Record record = additionalRecordLookup.lookup( sizeExp, id );
            if ( record != null )
            {
                // The additional lookup had this record so we're going to return here w/o touching the store
                // although do the same as below, i.e. return the record if it's in use, otherwise null
                return record.hasFlag( FLAG_IN_USE ) ? record : null;
            }
        }

>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        SimpleStore store = stores.mainStore( sizeExp );
        Record record = xRecord( sizeExp );
        // TODO depending on whether we're strict about it we should throw instead of returning false perhaps?
        return store.read( xCursor( sizeExp ), record, id ) && record.hasFlag( FLAG_IN_USE ) ? record : null;
    }

    private boolean ensureFreshDataInstanceForLoadingNewNode()
    {
<<<<<<< HEAD
        if ( header == null )
        {
            header = new Header();
        }

=======
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        boolean reused = true;
        if ( data != null )
        {
            reused = resetOrDereferenceData();
        }
        if ( data == null )
        {
            data = new FrekiCursorData( stores.getNumMainStores() );
        }
        return reused;
    }

    boolean initializeOtherCursorFromStateOfThisCursor( FrekiMainStoreCursor otherCursor )
    {
        if ( !data.isLoaded() )
        {
            return false;
        }
        otherCursor.data = data;
        data.refCount++;
        return true;
    }

<<<<<<< HEAD
    boolean initializeFromRecord( Record record )
    {
        if ( !record.hasFlag( FLAG_IN_USE ) )
        {
            return false;
        }

        ensureFreshDataInstanceForLoadingNewNode();
        if ( record.sizeExp() == 0 )
        {
            data.gatherDataFromX1( record );
            data.nodeId = record.id;
        }
        else
        {
            data.gatherDataFromXL( record );
            data.nodeId = data.backwardPointer;
        }
        return true;
    }

    ByteBuffer readRelationshipTypes()
    {
        ensureRelationshipsLoaded();
        if ( data.relationshipOffset == 0 )
        {
            relationshipTypesInNode = EMPTY_INT_ARRAY;
            return null;
        }
        else
        {
            ByteBuffer buffer = data.relationshipBuffer();
            relationshipTypesInNode = readIntDeltas( new IntArrayTarget(), buffer ).array();
            // Right after the types array the relationship group data starts, so this is the offset for the first type
            firstRelationshipTypeOffset = buffer.position();
            return buffer;
=======
    ByteBuffer readRelationshipTypes()
    {
        ensureRelationshipsLocated();
        ByteBuffer buffer = data.relationshipBuffer();
        readRelationshipTypes( buffer );
        return buffer;
    }

    void readRelationshipTypes( ByteBuffer buffer )
    {
        if ( buffer == null )
        {
            relationshipTypesInNode = EMPTY_INT_ARRAY;
        }
        else
        {
            relationshipTypesInNode = readInts( buffer, true );
            // Right after the types array the relationship group data starts, so this is the offset for the first type
            firstRelationshipTypeOffset = buffer.position();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        }
    }

    void readRelationshipTypesAndOffsets()
    {
        if ( readRelationshipTypes() == null )
        {
            relationshipTypeOffsets = EMPTY_INT_ARRAY;
        }
<<<<<<< HEAD
        else
        {
            relationshipTypeOffsets = readIntDeltas( new IntArrayTarget(), data.relationshipBuffer( data.relationshipTypeOffsetsOffset ) ).array();
=======
        else if ( !data.isDense )
        {
            relationshipTypeOffsets = readInts( data.relationshipBuffer( data.relationshipTypeOffsetsOffset ), true );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        }
    }

    int relationshipPropertiesOffset( ByteBuffer buffer, int relationshipGroupPropertiesOffset, int relationshipIndexInGroup )
    {
        if ( relationshipIndexInGroup == -1 )
        {
            return NULL_OFFSET;
        }

        checkState( relationshipGroupPropertiesOffset >= 0, "Should not be called if this relationship has no properties" );
        int offset = relationshipGroupPropertiesOffset;
        for ( int i = 0; i < relationshipIndexInGroup; i++ )
        {
            int blockSize = buffer.get( offset );
            offset += blockSize;
        }
        return offset + 1;
    }

    int relationshipTypeOffset( int typeIndex )
    {
        return typeIndex == 0 ? firstRelationshipTypeOffset : firstRelationshipTypeOffset + relationshipTypeOffsets[typeIndex - 1];
    }

    public void setForceLoad()
    {
        this.forceLoad = true;
    }

    @Override
    public void close()
    {
        IOUtils.closeAllUnchecked( xCursors );
        Arrays.fill( xCursors, null );
    }

    @Override
    public String toString()
    {
        return String.format( "%s,%s", getClass().getSimpleName(), data );
    }
}
