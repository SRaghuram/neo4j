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

import org.neo4j.internal.freki.StreamVByte.IntArrayTarget;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.isDenseFromForwardPointer;
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
    final PageCursorTracer cursorTracer;

    Record smallRecord;
    Record record;
    ByteBuffer data;
    private PageCursor cursor;
    long loadedNodeId = NULL;

    // state from record data header
    MainRecordHeaderState headerState;
    boolean borrowedHeaderState;

    // state from relationship section, it's here because both relationship cursors as well as property cursor makes use of them
    int[] relationshipTypesInNode;
    int[] relationshipTypeOffsets;
    int firstRelationshipTypeOffset;

    FrekiMainStoreCursor( MainStores stores, PageCursorTracer cursorTracer )
    {
        this.stores = stores;
        this.cursorTracer = cursorTracer;
        this.record = stores.mainStore.newRecord();
        this.smallRecord = record;
        reset();
    }

    public void reset()
    {
        data = null;
        relationshipTypeOffsets = null;
        relationshipTypesInNode = null;
        loadedNodeId = NULL;
        if ( borrowedHeaderState )
        {
            headerState = null;
            borrowedHeaderState = false;
        }
        else if ( headerState != null )
        {
            headerState.reset();
        }
    }

    PageCursor cursor()
    {
        if ( cursor == null )
        {
            cursor = stores.mainStore.openReadCursor();
        }
        return cursor;
    }

    /**
     * Loads a record from {@link Store}, starting with the "small" record, which corresponds to the given id.
     * The record header, i.e. offsets and such is read and if it looks like this record points to a larger record
     * then the larger record is also loaded. They will live in {@link #smallRecord} and {@link #record} respectively,
     * but {@link #data} will point to the large record data, which for the time being holds the majority of the data.
     */
    boolean loadMainRecord( long id )
    {
        stores.mainStore.read( cursor(), smallRecord, id );
        if ( !smallRecord.hasFlag( FLAG_IN_USE ) )
        {
            return false;
        }
        data = smallRecord.dataForReading();
        readOffsets( true );
        if ( headerState.containsForwardPointer && !headerState.isDense )
        {
            // Let's load the larger record
            int sizeExp = sizeExponentialFromForwardPointer( headerState.forwardPointer );
            SimpleStore largeStore = stores.mainStore( sizeExp );
            if ( record == null || record.sizeExp() != sizeExp )
            {
                record = largeStore.newRecord();
            }
            try ( PageCursor largeCursor = largeStore.openReadCursor() )
            {
                largeStore.read( largeCursor, record, idFromForwardPointer( headerState.forwardPointer ) );
                if ( !record.hasFlag( FLAG_IN_USE ) )
                {
                    return false;
                }
                data = record.dataForReading();
                readOffsets( false );
                if ( headerState.backPointer != id )
                {
                    throw new IllegalStateException( "Loaded X1 record for node:" + id + " with a linked record " + headerState.forwardPointer +
                            " that didn't point back, instead pointing to " + headerState.backPointer );
                }
            }
        }
        loadedNodeId = id;
        return true;
    }

    /**
     * Initializes another cursor with the state of this cursor, starting with the "small" record, which corresponds to the given id.
     * If a larger record is also involved then that too will be transferred. They will live in {@link #smallRecord} and
     * {@link #record} respectively, but {@link #data} will point to the large record data, which for the time being holds the majority of the data.
     */
    boolean initializeOtherCursorFromStateOfThisCursor( FrekiMainStoreCursor otherCursor )
    {
        if ( smallRecord.hasFlag( FLAG_IN_USE ) )
        {
            otherCursor.smallRecord.initializeFromSharedData( smallRecord );
            otherCursor.data = otherCursor.smallRecord.dataForReading();
            otherCursor.headerState = headerState;
            otherCursor.borrowedHeaderState = true;
            otherCursor.loadedNodeId = loadedNodeId;
            if ( headerState.containsForwardPointer && !headerState.isDense )
            {
                int sizeExp = sizeExponentialFromForwardPointer( headerState.forwardPointer );
                SimpleStore largeStore = stores.mainStore( sizeExp );
                if ( otherCursor.record == null || otherCursor.record.sizeExp() != sizeExp )
                {
                    otherCursor.record = largeStore.newRecord();
                }
                otherCursor.record.initializeFromSharedData( record );
                otherCursor.data = otherCursor.record.dataForReading();
            }
            return true;
        }
        return false;
    }

    boolean initializeFromRecord( Record record )
    {
        if ( !record.hasFlag( FLAG_IN_USE ) )
        {
            return false;
        }

        if ( record.sizeExp() == 0 )
        {
            smallRecord = record;
            data = record.dataForReading( 0 );
            readOffsets( true );
            loadedNodeId = record.id;
        }
        else
        {
            this.record = record;
            data = record.dataForReading( 0 );
            readOffsets( false );
            loadedNodeId = headerState.backPointer;
        }
        return true;
    }

    private void readOffsets( boolean forSmallRecord )
    {
        int offsetsHeader = MutableNodeRecordData.readOffsetsHeader( data );
        if ( headerState == null )
        {
            headerState = new MainRecordHeaderState();
        }
        if ( forSmallRecord )
        {
            headerState.initializeForSmallRecord( data.position() );
        }
        headerState.initialize( offsetsHeader );
        if ( forSmallRecord )
        {
            headerState.containsForwardPointer = MutableNodeRecordData.containsForwardPointer( offsetsHeader );
            if ( headerState.containsForwardPointer )
            {
                setDataPositionAfterTypeOffsets();
                headerState.forwardPointer = data.getLong();
                headerState.isDense = isDenseFromForwardPointer( headerState.forwardPointer );
            }
        }
        else
        {
            headerState.containsBackPointer = MutableNodeRecordData.containsBackPointer( offsetsHeader );
            if ( headerState.containsBackPointer )
            {
                data.position( headerState.endOffset );
                setDataPositionAfterTypeOffsets();
                headerState.backPointer = readLongs( data )[0];
            }
        }
    }

    private void setDataPositionAfterTypeOffsets()
    {
        data.position( headerState.endOffset );
        if ( headerState.relationshipsOffset != 0 )
        {
            // skip the type offsets array
            StreamVByte.readIntDeltas( SKIP, data );
        }
    }

    void readRelationshipTypesAndOffsets()
    {
        if ( headerState.relationshipsOffset == 0 )
        {
            relationshipTypesInNode = EMPTY_INT_ARRAY;
            relationshipTypeOffsets = EMPTY_INT_ARRAY;
            return;
        }

        data.position( headerState.relationshipsOffset );
        relationshipTypesInNode = readIntDeltas( new IntArrayTarget(), data ).array();
        // Right after the types array the relationship group data starts, so this is the offset for the first type
        firstRelationshipTypeOffset = data.position();

        // Then read the rest of the offsets for type indexes > 0 after all the relationship data groups, i.e. at endOffset
        // The values in the typeOffsets array are relative to the firstTypeOffset
        IntArrayTarget typeOffsetsTarget = new IntArrayTarget();
        readIntDeltas( typeOffsetsTarget, data.array(), headerState.endOffset );
        relationshipTypeOffsets = typeOffsetsTarget.array();
    }

    int relationshipPropertiesOffset( int relationshipGroupPropertiesOffset, int relationshipIndexInGroup )
    {
        if ( relationshipIndexInGroup == -1 )
        {
            return NULL_OFFSET;
        }

        checkState( relationshipGroupPropertiesOffset >= 0, "Should not be called if this relationship has no properties" );
        int offset = relationshipGroupPropertiesOffset;
        for ( int i = 0; i < relationshipIndexInGroup; i++ )
        {
            int blockSize = data.get( offset );
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
        if ( cursor != null )
        {
            cursor.close();
            cursor = null;
        }
    }
}
