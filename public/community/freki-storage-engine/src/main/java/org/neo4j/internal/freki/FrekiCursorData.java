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
<<<<<<< HEAD

import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.Header.FLAG_IS_DENSE;
=======
import java.util.Arrays;

import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.Header.FLAG_HAS_DENSE_RELATIONSHIPS;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import static org.neo4j.internal.freki.Header.FLAG_LABELS;
import static org.neo4j.internal.freki.Header.OFFSET_DEGREES;
import static org.neo4j.internal.freki.Header.OFFSET_PROPERTIES;
import static org.neo4j.internal.freki.Header.OFFSET_RECORD_POINTER;
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIPS;
<<<<<<< HEAD
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIP_TYPE_OFFSETS;
import static org.neo4j.internal.freki.MutableNodeRecordData.recordPointerToString;
import static org.neo4j.internal.freki.StreamVByte.readLongs;
=======
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS;
import static org.neo4j.internal.freki.IntermediateBuffer.PIECE_HEADER_SIZE;
import static org.neo4j.internal.freki.IntermediateBuffer.isFirstFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.isLastFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.ordinalFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.versionFromPieceHeader;
import static org.neo4j.internal.freki.MutableNodeData.forwardPointer;
import static org.neo4j.internal.freki.MutableNodeData.recordPointerToString;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

/**
 * Data that cursors need to read data. This is a minimal parsed version of data loaded to a {@link Record} from a {@link Store}.
 * Different types of data can exist in different records, so those groups come in pairs: [offset, buffer] where a buffer
 * can be shared with other pairs if e.g. labels and properties happens to live in the same buffer (i.e. same record).
 */
class FrekiCursorData
{
<<<<<<< HEAD
    Record[] records;
    private Header header = new Header();

    long nodeId = NULL;
    boolean x1Loaded;
    long forwardPointer = NULL;
    long backwardPointer = NULL;
    boolean isDense;
    boolean xLLoaded;

    int labelOffset;
    private ByteBuffer labelBuffer;
    int propertyOffset;
    private ByteBuffer propertyBuffer;
    int relationshipOffset;
    int relationshipTypeOffsetsOffset;
    private ByteBuffer relationshipBuffer;
=======
    private static final int RECORD_REUSE_NUM = 3;
    Record[][] records;
    int[] recordIndex;
    Header header = new Header();

    long nodeId = NULL;
    boolean x1Loaded;
    long xLChainStartPointer = NULL;
    long xLChainNextLinkPointer = NULL;
    boolean isDense;

    int labelOffset;
    private ByteBuffer labelBuffer;
    PieceLoadingState labelsSplitState;
    int propertyOffset;
    private ByteBuffer propertyBuffer;
    PieceLoadingState propertySplitState;
    int relationshipOffset;
    int relationshipTypeOffsetsOffset;
    private ByteBuffer relationshipBuffer;
    PieceLoadingState degreesSplitState;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

    int refCount = 1;

    FrekiCursorData( int numMainStores )
    {
<<<<<<< HEAD
        this.records = new Record[numMainStores];
    }

    void gatherDataFromX1( Record record )
=======
        this.records = new Record[numMainStores][RECORD_REUSE_NUM];
        this.records[0] = new Record[1];
        this.recordIndex = new int[numMainStores];
        Arrays.fill( recordIndex, -1 );
    }

    boolean gatherDataFromX1( Record record )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    {
        x1Loaded = true;
        ByteBuffer buffer = record.data( 0 );
        header.deserialize( buffer );
<<<<<<< HEAD
        assignDataOffsets( buffer );
        if ( header.hasOffset( OFFSET_RECORD_POINTER ) )
        {
            forwardPointer = readRecordPointer( buffer );
        }
    }

    void gatherDataFromXL( Record record )
    {
        xLLoaded = true;
        ByteBuffer buffer = record.data( 0 );
        header.deserialize( buffer );
        assignDataOffsets( buffer );
        assert header.hasOffset( OFFSET_RECORD_POINTER );
        backwardPointer = readRecordPointer( buffer );
    }

    private void assignDataOffsets( ByteBuffer x1Buffer )
    {
        if ( header.hasFlag( FLAG_IS_DENSE ) )
        {
            isDense = true;
        }
        if ( header.hasFlag( FLAG_LABELS ) )
        {
            labelOffset = x1Buffer.position();
            labelBuffer = x1Buffer;
        }
        if ( header.hasOffset( OFFSET_PROPERTIES ) )
        {
            propertyOffset = header.getOffset( OFFSET_PROPERTIES );
            propertyBuffer = x1Buffer;
        }
        if ( header.hasOffset( OFFSET_RELATIONSHIPS ) )
        {
            relationshipOffset = header.getOffset( OFFSET_RELATIONSHIPS );
            relationshipBuffer = x1Buffer;
            relationshipTypeOffsetsOffset = header.getOffset( OFFSET_RELATIONSHIP_TYPE_OFFSETS );
        }
        if ( header.hasOffset( OFFSET_DEGREES ) )
        {
            relationshipOffset = header.getOffset( OFFSET_DEGREES );
            relationshipBuffer = x1Buffer;
        }
    }

    private long readRecordPointer( ByteBuffer xLBuffer )
    {
        return readLongs( xLBuffer.position( header.getOffset( OFFSET_RECORD_POINTER ) ) )[0];
=======
        if ( !assignDataOffsets( buffer ) )
        {
            return false;
        }
        if ( header.hasMark( OFFSET_RECORD_POINTER ) )
        {
            xLChainStartPointer = forwardPointer( readRecordPointers( buffer ), false );
            xLChainNextLinkPointer = xLChainStartPointer;
        }
        return true;
    }

    boolean gatherDataFromXL( Record record )
    {
        ByteBuffer buffer = record.data( 0 );
        header.deserialize( buffer );
        if ( !assignDataOffsets( buffer ) )
        {
            return false;
        }
        assert header.hasMark( OFFSET_RECORD_POINTER );
        long[] pointers = readRecordPointers( buffer );
        xLChainNextLinkPointer = forwardPointer( pointers, true );
        return true;
    }

    private boolean assignDataOffsets( ByteBuffer buffer )
    {
        if ( header.hasMark( FLAG_HAS_DENSE_RELATIONSHIPS ) )
        {
            isDense = true;
        }
        if ( labelOffset == 0 && header.hasMark( FLAG_LABELS ) )
        {
            labelOffset = buffer.position();
            labelBuffer = buffer;
            if ( header.hasReferenceMark( FLAG_LABELS ) )
            {
                labelsSplitState = assignFirstPiece( labelBuffer.position( labelOffset ) );
                if ( labelsSplitState == null )
                {
                    return false;
                }
                labelOffset += PIECE_HEADER_SIZE;
            }
        }
        if ( propertyOffset == 0 && header.hasMark( OFFSET_PROPERTIES ) )
        {
            propertyOffset = header.getOffset( OFFSET_PROPERTIES );
            propertyBuffer = buffer;
            if ( header.hasReferenceMark( OFFSET_PROPERTIES ) )
            {
                propertySplitState = assignFirstPiece( propertyBuffer.position( propertyOffset ) );
                if ( propertySplitState == null )
                {
                    return false;
                }
                propertyOffset += PIECE_HEADER_SIZE;
            }
        }
        if ( header.hasMark( OFFSET_RELATIONSHIPS ) )
        {
            relationshipOffset = header.getOffset( OFFSET_RELATIONSHIPS );
            relationshipBuffer = buffer;
            relationshipTypeOffsetsOffset = header.getOffset( OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
        }
        if ( relationshipOffset == 0 && header.hasMark( OFFSET_DEGREES ) )
        {
            relationshipOffset = header.getOffset( OFFSET_DEGREES );
            relationshipBuffer = buffer;
            if ( header.hasReferenceMark( OFFSET_DEGREES ) )
            {
                degreesSplitState = assignFirstPiece( relationshipBuffer.position( relationshipOffset ) );
                if ( degreesSplitState == null )
                {
                    return false;
                }
                relationshipOffset += PIECE_HEADER_SIZE;
            }
        }
        return true;
    }

    private PieceLoadingState assignFirstPiece( ByteBuffer buffer )
    {
        short pieceHeader = buffer.getShort();
        if ( !isFirstFromPieceHeader( pieceHeader ) || ordinalFromPieceHeader( pieceHeader ) != 0 || isLastFromPieceHeader( pieceHeader ) )
        {
            return null;
        }
        return new PieceLoadingState( buffer, versionFromPieceHeader( pieceHeader ) );
    }

    private long[] readRecordPointers( ByteBuffer buffer )
    {
        return MutableNodeData.readRecordPointers( buffer.position( header.getOffset( OFFSET_RECORD_POINTER ) ) );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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

<<<<<<< HEAD
    ByteBuffer degreesBuffer()
    {
        return relationshipBuffer();
    }

    boolean isLoaded()
    {
        return x1Loaded || xLLoaded;
=======
    boolean isLoaded()
    {
        return x1Loaded;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    void reset()
    {
        assert refCount == 1;
        nodeId = NULL;
        x1Loaded = false;
<<<<<<< HEAD
        forwardPointer = NULL;
        backwardPointer = NULL;
        isDense = false;
        xLLoaded = false;
        labelOffset = 0;
        propertyOffset = 0;
        relationshipOffset = 0;
        relationshipTypeOffsetsOffset = 0;
=======
        xLChainStartPointer = NULL;
        xLChainNextLinkPointer = NULL;
        isDense = false;
        labelOffset = 0;
        labelsSplitState = null;
        propertyOffset = 0;
        propertySplitState = null;
        relationshipOffset = 0;
        relationshipTypeOffsetsOffset = 0;
        degreesSplitState = null;
        Arrays.fill( recordIndex, -1 );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public String toString()
    {
<<<<<<< HEAD
        return isLoaded() ? String.format( "NodeData[%d,fw:%s%s,lo:%d,po:%d,ro:%d]", nodeId, recordPointerToString( forwardPointer ),
                isDense ? "->DENSE" : "", labelOffset, propertyOffset, relationshipOffset )
                          : "<not loaded>";
    }
=======
        return isLoaded() ? String.format( "NodeData[%d,fw:%s%s,lo:%d,po:%d,ro:%d]", nodeId, recordPointerToString( xLChainStartPointer ),
                isDense ? "->DENSE" : "", labelOffset, propertyOffset, relationshipOffset )
                          : "<not loaded>";
    }

    /**
     * Struct used both for holding first piece as well as carrying over information for verification when loading other pieces later.
     */
    static class PieceLoadingState
    {
        Header header = new Header();
        final byte version;
        byte ordinal;
        boolean last;
        ByteBuffer buffer;
        private final ByteBuffer firstPieceBuffer;

        PieceLoadingState( ByteBuffer firstPieceBuffer, byte version )
        {
            this.firstPieceBuffer = firstPieceBuffer;
            this.version = version;
            reset();
        }

        void reset()
        {
            buffer = firstPieceBuffer;
            ordinal = 0;
            last = false;
        }
    }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
}
