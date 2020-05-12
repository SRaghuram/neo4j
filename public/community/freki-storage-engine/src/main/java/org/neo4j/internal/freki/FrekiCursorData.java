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

import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.Header.FLAG_HAS_DENSE_RELATIONSHIPS;
import static org.neo4j.internal.freki.Header.FLAG_LABELS;
import static org.neo4j.internal.freki.Header.OFFSET_DEGREES;
import static org.neo4j.internal.freki.Header.OFFSET_PROPERTIES;
import static org.neo4j.internal.freki.Header.OFFSET_RECORD_POINTER;
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIPS;
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS;
import static org.neo4j.internal.freki.MutableNodeData.recordPointerToString;
import static org.neo4j.internal.freki.StreamVByte.readLongs;

/**
 * Data that cursors need to read data. This is a minimal parsed version of data loaded to a {@link Record} from a {@link Store}.
 * Different types of data can exist in different records, so those groups come in pairs: [offset, buffer] where a buffer
 * can be shared with other pairs if e.g. labels and properties happens to live in the same buffer (i.e. same record).
 */
class FrekiCursorData
{
    Record[] records;
    Header header = new Header();

    long nodeId = NULL;
    boolean x1Loaded;
    long xLChainStartPointer = NULL;
    long xLChainNextLinkPointer = NULL;
    long x1Pointer = NULL;
    boolean isDense;
    boolean xLChainLoaded;

    int labelOffset;
    private ByteBuffer labelBuffer;
    int propertyOffset;
    private ByteBuffer propertyBuffer;
    int relationshipOffset;
    int relationshipTypeOffsetsOffset;
    private ByteBuffer relationshipBuffer;

    int refCount = 1;

    FrekiCursorData( int numMainStores )
    {
        this.records = new Record[numMainStores];
    }

    void gatherDataFromX1( Record record )
    {
        x1Loaded = true;
        ByteBuffer buffer = record.data( 0 );
        header.deserialize( buffer );
        assignDataOffsets( buffer );
        if ( header.hasMark( OFFSET_RECORD_POINTER ) )
        {
            xLChainStartPointer = readRecordPointers( buffer )[0];
            xLChainNextLinkPointer = xLChainStartPointer;
            // xL can be loaded lazily when/if needed
        }
        else
        {
            //We have no chain
            xLChainLoaded = true;
        }
    }

    void gatherDataFromXL( Record record )
    {
        ByteBuffer buffer = record.data( 0 );
        header.deserialize( buffer );
        assignDataOffsets( buffer );
        assert header.hasMark( OFFSET_RECORD_POINTER );
        long[] pointers = readRecordPointers( buffer );
        x1Pointer = pointers[0];
        xLChainLoaded = pointers.length == 1;
        xLChainNextLinkPointer = !xLChainLoaded ? pointers[1] : NULL;
    }

    private void assignDataOffsets( ByteBuffer buffer )
    {
        if ( header.hasMark( FLAG_HAS_DENSE_RELATIONSHIPS ) )
        {
            isDense = true;
        }
        if ( header.hasMark( FLAG_LABELS ) )
        {
            labelOffset = buffer.position();
            labelBuffer = buffer;
        }
        if ( header.hasMark( OFFSET_PROPERTIES ) )
        {
            propertyOffset = header.getOffset( OFFSET_PROPERTIES );
            propertyBuffer = buffer;
        }
        if ( header.hasMark( OFFSET_RELATIONSHIPS ) )
        {
            relationshipOffset = header.getOffset( OFFSET_RELATIONSHIPS );
            relationshipBuffer = buffer;
            relationshipTypeOffsetsOffset = header.getOffset( OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
        }
        if ( header.hasMark( OFFSET_DEGREES ) )
        {
            relationshipOffset = header.getOffset( OFFSET_DEGREES );
            relationshipBuffer = buffer;
        }
    }

    private long[] readRecordPointers( ByteBuffer buffer )
    {
        return readLongs( buffer.position( header.getOffset( OFFSET_RECORD_POINTER ) ) );
    }

    ByteBuffer labelBuffer()
    {
        return labelOffset == 0 ? null : labelBuffer.position( labelOffset );
    }

    ByteBuffer propertyBuffer()
    {
        return propertyOffset == 0 ? null : propertyBuffer.position( propertyOffset );
    }

    ByteBuffer relationshipBuffer()
    {
        return relationshipOffset == 0 ? null : relationshipBuffer.position( relationshipOffset );
    }

    ByteBuffer relationshipBuffer( int offset )
    {
        return relationshipBuffer.position( offset );
    }

    ByteBuffer degreesBuffer()
    {
        return relationshipBuffer();
    }

    boolean isLoaded()
    {
        return x1Loaded | xLChainLoaded;
    }

    void reset()
    {
        assert refCount == 1;
        nodeId = NULL;
        x1Loaded = false;
        xLChainStartPointer = NULL;
        xLChainNextLinkPointer = NULL;
        x1Pointer = NULL;
        isDense = false;
        xLChainLoaded = false;
        labelOffset = 0;
        propertyOffset = 0;
        relationshipOffset = 0;
        relationshipTypeOffsetsOffset = 0;
    }

    @Override
    public String toString()
    {
        return isLoaded() ? String.format( "NodeData[%d,fw:%s%s,lo:%d,po:%d,ro:%d]", nodeId, recordPointerToString( xLChainStartPointer ),
                isDense ? "->DENSE" : "", labelOffset, propertyOffset, relationshipOffset )
                          : "<not loaded>";
    }
}
