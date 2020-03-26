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
import static org.neo4j.internal.freki.Header.FLAG_IS_DENSE;
import static org.neo4j.internal.freki.Header.FLAG_LABELS;
import static org.neo4j.internal.freki.Header.OFFSET_DEGREES;
import static org.neo4j.internal.freki.Header.OFFSET_PROPERTIES;
import static org.neo4j.internal.freki.Header.OFFSET_RECORD_POINTER;
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIPS;
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIP_TYPE_OFFSETS;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
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

    private Header header;
    FrekiCursorData data;
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
        if ( data.records[sizeExp] == null )
        {
            data.records[sizeExp] = stores.mainStore( sizeExp ).newRecord();
        }
        return data.records[sizeExp];
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
        Record x1Record = xRecord( 0 );
        if ( !stores.mainStore.read( xCursor( 0 ), x1Record, nodeId ) || !x1Record.hasFlag( FLAG_IN_USE ) )
        {
            return false;
        }

        // From x1 read forward pointer, labels offset and potentially other offsets too
        data.nodeId = nodeId;
        gatherDataFromX1( x1Record );
        if ( data.forwardPointer != NULL )
        {
            int sizeExp = sizeExponentialFromRecordPointer( data.forwardPointer );
            SimpleStore xLStore = stores.mainStore( sizeExp );
            Record xLRecord = xRecord( sizeExp );
            if ( !xLStore.read( xCursor( sizeExp ), xLRecord, idFromRecordPointer( data.forwardPointer ) ) || !xLRecord.hasFlag( FLAG_IN_USE ) )
            {
                // TODO depending on whether we're strict about it we should throw here perhaps?
                return false;
            }
            gatherDataFromXL( xLRecord );
        }
        return true;
    }

    private boolean ensureFreshDataInstanceForLoadingNewNode()
    {
        if ( header == null )
        {
            header = new Header();
        }

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

    private void gatherDataFromX1( Record x1Record )
    {
        data.x1Loaded = true;
        ByteBuffer x1Buffer = x1Record.data( 0 );
        header.deserialize( x1Buffer );
        assignDataOffsets( x1Buffer );
        if ( header.hasOffset( OFFSET_RECORD_POINTER ) )
        {
            data.forwardPointer = readRecordPointer( x1Buffer );
        }
    }

    private void gatherDataFromXL( Record xLRecord )
    {
        data.xLLoaded = true;
        ByteBuffer xLBuffer = xLRecord.data( 0 );
        header.deserialize( xLBuffer );
        assignDataOffsets( xLBuffer );
        assert header.hasOffset( OFFSET_RECORD_POINTER );
        data.backwardPointer = readRecordPointer( xLBuffer );
    }

    private long readRecordPointer( ByteBuffer xLBuffer )
    {
        return readLongs( xLBuffer.position( header.getOffset( OFFSET_RECORD_POINTER ) ) )[0];
    }

    private void assignDataOffsets( ByteBuffer x1Buffer )
    {
        if ( header.hasFlag( FLAG_IS_DENSE ) )
        {
            data.isDense = true;
        }
        if ( header.hasFlag( FLAG_LABELS ) )
        {
            data.assignLabelOffset( x1Buffer.position(), x1Buffer );
        }
        if ( header.hasOffset( OFFSET_PROPERTIES ) )
        {
            data.assignPropertyOffset( header.getOffset( OFFSET_PROPERTIES ), x1Buffer );
        }
        if ( header.hasOffset( OFFSET_RELATIONSHIPS ) )
        {
            data.assignRelationshipOffset( header.getOffset( OFFSET_RELATIONSHIPS ), x1Buffer, header.getOffset( OFFSET_RELATIONSHIP_TYPE_OFFSETS ) );
        }
        if ( header.hasOffset( OFFSET_DEGREES ) )
        {
            data.assignDegreesOffset( header.getOffset( OFFSET_DEGREES ), x1Buffer );
        }
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

    boolean initializeFromRecord( Record record )
    {
        if ( !record.hasFlag( FLAG_IN_USE ) )
        {
            return false;
        }

        ensureFreshDataInstanceForLoadingNewNode();
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

    ByteBuffer readRelationshipTypes()
    {
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
        }
    }

    void readRelationshipTypesAndOffsets()
    {
        if ( readRelationshipTypes() == null )
        {
            relationshipTypeOffsets = EMPTY_INT_ARRAY;
        }
        else
        {
            relationshipTypeOffsets = readIntDeltas( new IntArrayTarget(), data.relationshipBuffer( data.relationshipTypeOffsetsOffset ) ).array();
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
