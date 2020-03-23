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

import org.neo4j.internal.freki.StreamVByte.IntArrayTarget;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.internal.freki.MutableNodeRecordData.containsBackPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.containsForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.endOffset;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointerPointsToXRecord;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.propertyOffset;
import static org.neo4j.internal.freki.MutableNodeRecordData.readOffsetsHeader;
import static org.neo4j.internal.freki.MutableNodeRecordData.relationshipOffset;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromForwardPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.SKIP;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
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

    FrekiCursorData data;
    boolean sharedData;
    // State from relationship section, it's here because both relationship cursors as well as property cursor makes use of them
    int[] relationshipTypesInNode;
    int[] relationshipTypeOffsets;
    int firstRelationshipTypeOffset;

    private PageCursor[] xCursors;

    FrekiMainStoreCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer )
    {
        this.stores = stores;
        this.cursorAccessPatternTracer = cursorAccessPatternTracer;
        this.cursorAccessTracer = cursorAccessPatternTracer.access();
        this.cursorTracer = cursorTracer;
        this.xCursors = new PageCursor[stores.getNumMainStores()];
        reset();
    }

    void reset()
    {
        if ( data != null )
        {
            if ( sharedData )
            {
                data = null;
            }
            else
            {
                data.reset();
            }
        }
        relationshipTypeOffsets = null;
        relationshipTypesInNode = null;
        firstRelationshipTypeOffset = 0;
    }

    PageCursor xCursor( int sizeExp )
    {
        if ( xCursors[sizeExp] == null )
        {
            xCursors[sizeExp] = stores.mainStore( sizeExp ).openReadCursor( cursorTracer );
        }
        return xCursors[sizeExp];
    }

    Record x1Record()
    {
        return stores.mainStore.newRecord();
    }

    /**
     * Loads raw byte contents and does minimal parsing of higher-level offsets of the given node into {@link #data}.
     * @param nodeId node id to load.
     * @return {@code true} if the node exists, otherwise {@code false}.
     */
    boolean load( long nodeId )
    {
        Record x1Record = x1Record();
        if ( !stores.mainStore.read( xCursor( 0 ), x1Record, nodeId ) || !x1Record.hasFlag( FLAG_IN_USE ) )
        {
            return false;
        }

        // From x1 read forward pointer, labels offset and potentially other offsets too
        ensureFreshDataInstance();
        data.nodeId = nodeId;
        gatherDataFromX1( x1Record );
        if ( forwardPointerPointsToXRecord( data.forwardPointer ) )
        {
            int sizeExp = sizeExponentialFromForwardPointer( data.forwardPointer );
            SimpleStore xLStore = stores.mainStore( sizeExp );
            Record xLRecord = xLStore.newRecord();
            if ( !xLStore.read( xCursor( sizeExp ), xLRecord, idFromForwardPointer( data.forwardPointer ) ) || !xLRecord.hasFlag( FLAG_IN_USE ) )
            {
                // TODO depending on whether we're strict about it we should throw here perhaps?
                return false;
            }
            gatherDataFromXL( xLRecord );
        }
        return true;
    }

    private void ensureFreshDataInstance()
    {
        if ( data == null )
        {
            data = new FrekiCursorData();
        }
        else
        {
            data.reset();
        }
    }

    private void gatherDataFromX1( Record x1Record )
    {
        data.x1Loaded = true;
        ByteBuffer x1Buffer = x1Record.data( 0 );
        int x1OffsetsHeader = readOffsetsHeader( x1Buffer );
        data.assignLabelOffset( x1Buffer.position(), x1Buffer );
        data.assignPropertyOffset( propertyOffset( x1OffsetsHeader ), x1Buffer );
        data.assignRelationshipOffset( relationshipOffset( x1OffsetsHeader ), x1Buffer );
        data.endOffset = endOffset( x1OffsetsHeader );

        // Check if there's more data to load from a larger x record
        // TODO an optimization would be to load xL lazily instead (e.g. if you would only need labels)
        if ( containsForwardPointer( x1OffsetsHeader ) )
        {
            x1Buffer.position( endOffset( x1OffsetsHeader ) );
            if ( data.relationshipOffset > 0 )
            {
                readIntDeltas( SKIP, x1Buffer );
            }
            data.forwardPointer = readLongs( x1Buffer )[0];
            assert data.forwardPointer != NULL;
        }
    }

    private void gatherDataFromXL( Record xLRecord )
    {
        data.xLLoaded = true;
        ByteBuffer xLBuffer = xLRecord.data( 0 );
        int xLOffsetsHeader = readOffsetsHeader( xLBuffer );
        data.assignPropertyOffset( propertyOffset( xLOffsetsHeader ), xLBuffer );
        data.assignRelationshipOffset( relationshipOffset( xLOffsetsHeader ), xLBuffer );

        assert containsBackPointer( xLOffsetsHeader );
        if ( data.relationshipOffset > 0 )
        {
            data.endOffset = endOffset( xLOffsetsHeader );
        }
    }

    boolean initializeOtherCursorFromStateOfThisCursor( FrekiMainStoreCursor otherCursor )
    {
        if ( !data.isLoaded() )
        {
            return false;
        }
        otherCursor.ensureFreshDataInstance();
        otherCursor.data.copyFrom( data );
        return true;
    }

    boolean initializeFromRecord( Record record )
    {
        if ( !record.hasFlag( FLAG_IN_USE ) )
        {
            return false;
        }

        ensureFreshDataInstance();
        if ( record.sizeExp() == 0 )
        {
            gatherDataFromX1( record );
            data.nodeId = record.id;
        }
        else
        {
            gatherDataFromXL( record );
            data.nodeId = data.backwardPointer;
        }
        return true;
    }

    void readRelationshipTypesAndOffsets()
    {
        if ( data.relationshipOffset == 0 )
        {
            relationshipTypesInNode = EMPTY_INT_ARRAY;
            relationshipTypeOffsets = EMPTY_INT_ARRAY;
            return;
        }

        ByteBuffer buffer = data.relationshipBuffer( data.relationshipOffset + 1 /*the HAS_DEGREES byte*/ );
        relationshipTypesInNode = readIntDeltas( new IntArrayTarget(), buffer ).array();
        // Right after the types array the relationship group data starts, so this is the offset for the first type
        firstRelationshipTypeOffset = buffer.position();

        // Then read the rest of the offsets for type indexes > 0 after all the relationship data groups, i.e. at endOffset
        // The values in the typeOffsets array are relative to the firstTypeOffset
        IntArrayTarget typeOffsetsTarget = new IntArrayTarget();
        readIntDeltas( typeOffsetsTarget, buffer.array(), data.endOffset );
        relationshipTypeOffsets = typeOffsetsTarget.array();
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
