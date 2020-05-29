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
import java.util.function.ToIntFunction;

import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.internal.freki.MutableNodeData.forwardPointer;
import static org.neo4j.internal.freki.MutableNodeData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.recordPointerToString;
import static org.neo4j.internal.freki.MutableNodeData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.readInts;
import static org.neo4j.internal.freki.StreamVByte.readLongs;
import static org.neo4j.util.Preconditions.checkState;

abstract class FrekiMainStoreCursor implements AutoCloseable
{
    static final long NULL = -1;
    static final int NULL_OFFSET = 0;

    final MainStores stores;
    final CursorAccessPatternTracer cursorAccessPatternTracer;
    final CursorAccessPatternTracer.ThreadAccess cursorAccessTracer;
    final PageCursorTracer cursorTracer;
    final MemoryTracker memoryTracker;
    boolean forceLoad;
    RecordLookup additionalRecordLookup;

    FrekiCursorData data;
    // State from relationship section, it's here because both relationship cursors as well as property cursor makes use of them
    int[] relationshipTypesInNode;
    int[] relationshipTypeOffsets;
    int firstRelationshipTypeOffset;

    private PageCursor[] xCursors;

    FrekiMainStoreCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this.stores = stores;
        this.cursorAccessPatternTracer = cursorAccessPatternTracer;
        this.cursorAccessTracer = cursorAccessPatternTracer.access();
        this.cursorTracer = cursorTracer;
        this.xCursors = new PageCursor[stores.getNumMainStores()];
        this.memoryTracker = memoryTracker;
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
        additionalRecordLookup = null;
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
        if ( x1Record == null )
        {
            return false;
        }

        data.nodeId = nodeId;
        data.gatherDataFromX1( x1Record );
        return true;
    }

    private void ensureX1Loaded()
    {
        if ( !data.x1Loaded )
        {
            Record x1Record = loadRecord( 0, data.nodeId );
            if ( x1Record != null )
            {
                data.gatherDataFromX1( x1Record );
            }
        }
    }

    //Package-private for testing/analysis purposes
    boolean loadNextChainLink()
    {
        //This is only capable of reading chain in one direction, until reached end.
        if ( !data.xLChainLoaded && data.xLChainNextLinkPointer != NULL )
        {
            int sizeExp = sizeExponentialFromRecordPointer( data.xLChainNextLinkPointer );
            Record xLRecord = loadRecord( sizeExp, idFromRecordPointer( data.xLChainNextLinkPointer ) );
            if ( xLRecord != null )
            {
                data.gatherDataFromXL( xLRecord );
                return true;
            }
        }
        return false;
    }

    /**
     * Provides convenient way of iterating through data that is split into multiple records. The {@link FrekiCursorData} keeps a reference
     * to the first (in chain) occurrence of a certain data part, i.e. labels or properties. Then when actually reading the data of that part
     * this is the buffer to start from and to, when it runs out, advance to the next buffer containing more data for that part this
     * method is invoked to get there. Very convenient, but a bit wasteful since it loads records one more time and then throws them away.
     * This is done so that the book keeping for where data for various parts are is as slim as possible to not penalise the non-split case,
     * which btw is by far the most common one, so if this split-reading is slightly suboptimal then that's fine a.t.m.
     *
     * @param buffer the buffer containing a piece of the data part that was just read. This buffer also contains information about how to get to
     * the next record in the chain. This information is used to load the next record and return a buffer positioned to start reading the next
     * piece of this part.
     * @param headerSlot which part to look for.
     * @return ByteBuffer (may be a different one than was passed in) filled with data and positioned to read the next piece of this data part,
     * or {@code null} if there were no more pieces to load for this part.
     */
    ByteBuffer loadNextSplitPiece( ByteBuffer buffer, int headerSlot )
    {
        Header header = new Header();
        header.deserialize( buffer.position( 0 ) );
        long forwardPointer;
        while ( (forwardPointer = forwardPointer( readLongs( buffer.position( header.getOffset( Header.OFFSET_RECORD_POINTER ) ) ),
                buffer.limit() > stores.mainStore.recordDataSize() )) != NULL )
        {
            int sizeExp = sizeExponentialFromRecordPointer( forwardPointer );
            Record record = new Record( sizeExp );
            if ( !stores.mainStore( sizeExp ).read( xCursor( sizeExp ), record, idFromRecordPointer( forwardPointer ) ) || !record.hasFlag( FLAG_IN_USE ) )
            {
                throw new IllegalStateException(
                        "Wanted to follow forward pointer " + recordPointerToString( forwardPointer ) + ", but ended up on an unused record" );
            }
            buffer = record.data();
            header.deserialize( buffer );
            if ( header.hasMark( headerSlot ) )
            {
                return buffer.position( header.getOffset( headerSlot ) );
            }
            // else there could be another record in between the pieces for this data part, so just skip on through it
        }
        return null;
    }

    private void ensureLoaded( ToIntFunction<FrekiCursorData> test, int headerSlot )
    {
        if ( (!data.x1Loaded || !data.xLChainLoaded) && test.applyAsInt( data ) == 0 )
        {
            ensureX1Loaded();
            while ( test.applyAsInt( data ) == 0 && data.header.hasReferenceMark( headerSlot ) )
            {
                if ( !loadNextChainLink() )
                {
                    //We traversed the rest of the chain. Either the header/store is corrupt or we did not start at the beginning of the chain.
                    throw new IllegalStateException( String.format( "Should have found header slot %d in record chain for node %d", headerSlot, data.nodeId ) );
                }
            }
        }
    }

    void ensureLabelsLocated()
    {
        ensureLoaded( data -> data.labelOffset, Header.FLAG_LABELS );
    }

    void ensureRelationshipsLocated()
    {
        ensureLoaded( data -> data.relationshipOffset, data.isDense ? Header.OFFSET_DEGREES : Header.OFFSET_RELATIONSHIPS );
    }

    void ensurePropertiesLocated()
    {
        ensureLoaded( data -> data.propertyOffset, Header.OFFSET_PROPERTIES );
    }

    private Record loadRecord( int sizeExp, long id )
    {
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

        SimpleStore store = stores.mainStore( sizeExp );
        Record record = xRecord( sizeExp );
        // TODO depending on whether we're strict about it we should throw instead of returning false perhaps?
        return store.read( xCursor( sizeExp ), record, id ) && record.hasFlag( FLAG_IN_USE ) ? record : null;
    }

    private boolean ensureFreshDataInstanceForLoadingNewNode()
    {
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
        }
    }

    void readRelationshipTypesAndOffsets()
    {
        if ( readRelationshipTypes() == null )
        {
            relationshipTypeOffsets = EMPTY_INT_ARRAY;
        }
        else if ( !data.isDense )
        {
            relationshipTypeOffsets = readInts( data.relationshipBuffer( data.relationshipTypeOffsetsOffset ), true );
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
