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

import static java.lang.Integer.min;

/**
 * Writes arrays of ints or longs. Terminology:
 * <ul>
 *     <li>chunk: count header (0-127) + 0-127 array items. A chunk contains 0 or more blocks</li>
 *     <li>block: count header (for 0-4 array items) + 0-4 array items</li>
 * </ul>
 */
class StreamVByte
{
    private static final int MASK_SQUASHED_BLOCK = 0b1000_0000;
    private static final int SHIFT_SQUASHED_BLOCK_LENGTH = 5;
    private static final int[] LONG_SIZES = {3, 4, 5, 7};

    static void writeIntDeltas( int[] source, ByteBuffer buffer )
    {
        writeIntDeltas( new IntArraySource( source ), buffer );
    }

    static void writeIntDeltas( Source source, ByteBuffer buffer )
    {
        byte[] serialized = buffer.array();
        int offset = buffer.position();
        int count = source.length();
        for ( int i = 0, prev = 0; i < count; )
        {
            int currentChunkCount = min( Byte.MAX_VALUE, count - i );
            int headerOffset = writeChunkHeader( serialized, offset, currentChunkCount );
            offset = headerOffset + 1;
            for ( int c = 0; c < currentChunkCount; )
            {
                int currentBlockCount = min( 4, currentChunkCount - c );
                for ( int j = 0; j < currentBlockCount; j++, i++, c++ )
                {
                    int value = source.valueAt( i );
                    offset = writeIntValue( serialized, offset, headerOffset, j, value - prev );
                    prev = value;
                }
                if ( currentBlockCount == 4 )
                {
                    headerOffset = offset++;
                    serialized[headerOffset] = 0; // clear the header byte since we could be overwriting previous data
                }
            }
        }
        if ( count % 127 == 0 )
        {
            serialized[offset++] = (byte) MASK_SQUASHED_BLOCK;
        }
        buffer.position( offset );
    }

    private static int writeChunkHeader( byte[] serialized, int offset, int currentBlockValueLength )
    {
        if ( currentBlockValueLength <= 2 )
        {
            // If block size is 0..2 then count and header bytes can be squashed into a single byte
            serialized[offset] = (byte) (MASK_SQUASHED_BLOCK | (currentBlockValueLength << SHIFT_SQUASHED_BLOCK_LENGTH));
        }
        else
        {
            serialized[offset++] = (byte) currentBlockValueLength;
            serialized[offset] = 0; // clear the header byte since we could be overwriting previous data
        }
        return offset;
    }

    private static int writeIntValue( byte[] serialized, int offset, int headerOffset, int j, int value )
    {
        if ( (value & 0xFF000000) != 0 )
        {
            serialized[headerOffset] |= 0b11 << (j * 2);
            serialized[offset++] = (byte) value;
            serialized[offset++] = (byte) (value >>> 8);
            serialized[offset++] = (byte) (value >>> 16);
            serialized[offset++] = (byte) (value >>> 24);
        }
        else if ( (value & 0xFF0000) != 0 )
        {
            serialized[headerOffset] |= 0b10 << (j * 2);
            serialized[offset++] = (byte) value;
            serialized[offset++] = (byte) (value >>> 8);
            serialized[offset++] = (byte) (value >>> 16);
        }
        else if ( (value & 0xFF00) != 0 )
        {
            serialized[headerOffset] |= 0b01 << (j * 2);
            serialized[offset++] = (byte) value;
            serialized[offset++] = (byte) (value >>> 8);
        }
        else
        {
            // header 2b not set, leaving it as 0b00
            serialized[offset++] = (byte) value;
        }
        return offset;
    }

    static <TARGET extends Target> TARGET readIntDeltas( TARGET target, ByteBuffer buffer )
    {
        byte[] serialized = buffer.array();
        int offset = buffer.position();
        buffer.position( readIntDeltas( target, serialized, offset ) );
        return target;
    }

    static int readIntDeltas( Target target, byte[] serialized, int offset )
    {
        int currentChunkCount = Byte.MAX_VALUE;
        for ( int i = 0, prev = 0; currentChunkCount == Byte.MAX_VALUE; )
        {
            int headerByte = unsigned( serialized[offset++] );
            if ( (headerByte & MASK_SQUASHED_BLOCK) != 0 )
            {
                // The special 0-2 header and count squashed byte
                currentChunkCount = ((headerByte & 0b0110_0000) >>> SHIFT_SQUASHED_BLOCK_LENGTH) & 0b11;
                headerByte &= 0b1111;
            }
            else
            {
                currentChunkCount = headerByte;
                headerByte = unsigned( serialized[offset++] );
            }

            target.beginBlock( currentChunkCount, i + currentChunkCount );
            for ( int c = 0; c < currentChunkCount; )
            {
                int currentBlockCount = min( 4, currentChunkCount - c );
                for ( int j = 0; j < currentBlockCount; j++, i++, c++ )
                {
                    int size = (headerByte >>> (j * 2)) & 0b11;
                    int readValue = readIntValue( serialized, offset, size );
                    int value = prev + readValue;
                    target.accept( i, value );
                    offset += size + 1; // because e.g. size==0 uses 1B, size==1 uses 2B a.s.o.
                    prev = value;
                }
                if ( currentBlockCount == 4 )
                {
                    headerByte = unsigned( serialized[offset++] );
                }
            }
        }
        return offset;
    }

    static boolean hasNonEmptyIntArray( ByteBuffer data )
    {
        byte header = data.get( data.position() );
        if ( (header & MASK_SQUASHED_BLOCK) != 0 )
        {
            return (((header & 0b0110_0000) >>> SHIFT_SQUASHED_BLOCK_LENGTH) & 0b11) > 0;
        }
        return header > 0;
    }

    private static int readIntValue( byte[] serialized, int offset, int size )
    {
        if ( size == 3 )
        {
            return unsigned( serialized[offset] ) |
                    (unsigned( serialized[offset + 1] ) << 8) |
                    (unsigned( serialized[offset + 2] ) << 16) |
                    (unsigned( serialized[offset + 3] ) << 24);
        }
        else if ( size == 2 )
        {
            return unsigned( serialized[offset] ) |
                    (unsigned( serialized[offset + 1] ) << 8) |
                    (unsigned( serialized[offset + 2] ) << 16);
        }
        else if ( size == 1 )
        {
            return unsigned( serialized[offset] ) |
                    (unsigned( serialized[offset + 1] ) << 8);
        }
        return unsigned( serialized[offset] );
    }

    /**
     * @return the size of the int-deltas block found at {@code offset} in {@code serialized} such that {@code offset} + the returned value
     * will given the offset right after this block. So this method is good for skipping a int-deltas block.
     */
    static int sizeOfIntDeltas( byte[] serialized, int offset )
    {
        int startOffset = offset;
        int currentChunkCount = Byte.MAX_VALUE;
        while ( currentChunkCount == Byte.MAX_VALUE )
        {
            int headerByte = unsigned( serialized[offset++] );
            if ( (headerByte & MASK_SQUASHED_BLOCK) != 0 )
            {
                // The special 0-2 header and count squashed byte
                currentChunkCount = ((headerByte & 0b0110_0000) >>> SHIFT_SQUASHED_BLOCK_LENGTH) & 0b11;
                headerByte &= 0b1111;
            }
            else
            {
                currentChunkCount = headerByte;
                headerByte = unsigned( serialized[offset++] );
            }

            for ( int c = 0; c < currentChunkCount; )
            {
                int currentBlockCount = min( 4, currentChunkCount - c );
                for ( int j = 0; j < currentBlockCount; j++, c++ )
                {
                    int size = (headerByte >>> (j * 2)) & 0b11;
                    offset += size + 1; // because e.g. size==0 uses 1B, size==1 uses 2B a.s.o.
                }
                if ( currentBlockCount == 4 )
                {
                    headerByte = unsigned( serialized[offset++] );
                }
            }
        }
        return offset - startOffset;
    }

    static boolean nonEmptyIntDeltas( byte[] serialized, int offset )
    {
        int headerByte = serialized[offset] & 0xFF;
        return (headerByte & MASK_SQUASHED_BLOCK) != 0
               ? (((headerByte & 0b0110_0000) >>> SHIFT_SQUASHED_BLOCK_LENGTH) & 0b11) > 0
               : headerByte > 0;
    }

    // =========== LONGS =============

    static void writeLongs( long[] source, ByteBuffer buffer )
    {
        byte[] serialized = buffer.array();
        int offset = buffer.position();
        int count = source.length;
        for ( int i = 0; i < count; )
        {
            int currentChunkCount = min( Byte.MAX_VALUE, count - i );
            int headerOffset = writeChunkHeader( serialized, offset, currentChunkCount );
            offset = headerOffset + 1;
            for ( int c = 0; c < currentChunkCount; )
            {
                int currentBlockCount = min( 4, currentChunkCount - c );
                for ( int j = 0; j < currentBlockCount; j++, i++, c++ )
                {
                    offset = writeLongValue( serialized, offset, headerOffset, j, source[i] );
                }
                if ( currentBlockCount == 4 )
                {
                    headerOffset = offset++;
                    serialized[headerOffset] = 0; // clear the header byte since we could be overwriting previous data
                }
            }
        }
        if ( count % 127 == 0 )
        {
            serialized[offset++] = (byte) MASK_SQUASHED_BLOCK;
        }
        buffer.position( offset );
    }

    static int calculateLongsSize( long[] source )
    {
        int size = 0;
        int count = source.length;
        for ( int i = 0; i < count; )
        {
            int currentChunkCount = min( Byte.MAX_VALUE, count - i );
            size += currentChunkCount <= 2 ? 1 : 2;
            for ( int c = 0; c < currentChunkCount; )
            {
                int currentBlockCount = min( 4, currentChunkCount - c );
                for ( int j = 0; j < currentBlockCount; j++, i++, c++ )
                {
                    size += calculateLongSize( source[i] );
                }
                if ( currentBlockCount == 4 )
                {
                    size++;
                }
            }
        }
        return count == 0 ? size + 1 : size;
    }

    private static int calculateLongSize( long value )
    {
        if ( (value & 0xFFFF00_00000000L) != 0 )
        {
            return LONG_SIZES[3];
        }
        else if ( (value & 0xFF_00000000L) != 0 )
        {
            return LONG_SIZES[2];
        }
        else if ( (value & 0xFF000000L) != 0 )
        {
            return LONG_SIZES[1];
        }
        return LONG_SIZES[0];
    }

    static long[] readLongs( ByteBuffer buffer )
    {
        byte[] serialized = buffer.array();
        int offset = buffer.position();
        int currentChunkCount = Byte.MAX_VALUE;
        long[] target = null;
        for ( int i = 0; currentChunkCount == Byte.MAX_VALUE; )
        {
            int headerByte = unsigned( serialized[offset++] );
            if ( (headerByte & MASK_SQUASHED_BLOCK) != 0 )
            {
                // The special 0-2 header and count squashed byte
                currentChunkCount = ((headerByte & 0b0110_0000) >>> SHIFT_SQUASHED_BLOCK_LENGTH) & 0b11;
                headerByte &= 0b1111;
            }
            else
            {
                currentChunkCount = headerByte;
                headerByte = unsigned( serialized[offset++] );
            }

            target = target == null ? new long[currentChunkCount] : Arrays.copyOf( target, i + currentChunkCount );
            for ( int c = 0; c < currentChunkCount; )
            {
                int currentBlockCount = min( 4, currentChunkCount - c );
                for ( int j = 0; j < currentBlockCount; j++, i++, c++ )
                {
                    int size = (headerByte >>> (j * 2)) & 0b11;
                    long readValue = readLongValue( serialized, offset, size );
                    target[i] = readValue;
                    offset += LONG_SIZES[size];
                }
                if ( currentBlockCount == 4 )
                {
                    headerByte = unsigned( serialized[offset++] );
                }
            }
        }
        buffer.position( offset );
        return target;
    }

    private static int writeLongValue( byte[] serialized, int offset, int headerOffset, int j, long value )
    {
        if ( (value & 0xFFFF00_00000000L) != 0 )
        {
            serialized[headerOffset] |= 0b11 << (j * 2);
            serialized[offset++] = (byte) value;
            serialized[offset++] = (byte) (value >>> 8);
            serialized[offset++] = (byte) (value >>> 16);
            serialized[offset++] = (byte) (value >>> 24);
            serialized[offset++] = (byte) (value >>> 32);
            serialized[offset++] = (byte) (value >>> 40);
            serialized[offset++] = (byte) (value >>> 48);
        }
        else if ( (value & 0xFF_00000000L) != 0 )
        {
            serialized[headerOffset] |= 0b10 << (j * 2);
            serialized[offset++] = (byte) value;
            serialized[offset++] = (byte) (value >>> 8);
            serialized[offset++] = (byte) (value >>> 16);
            serialized[offset++] = (byte) (value >>> 24);
            serialized[offset++] = (byte) (value >>> 32);
        }
        else if ( (value & 0xFF000000L) != 0 )
        {
            serialized[headerOffset] |= 0b01 << (j * 2);
            serialized[offset++] = (byte) value;
            serialized[offset++] = (byte) (value >>> 8);
            serialized[offset++] = (byte) (value >>> 16);
            serialized[offset++] = (byte) (value >>> 24);
        }
        else
        {
            // header 2b not set, leaving it as 0b00
            serialized[offset++] = (byte) value;
            serialized[offset++] = (byte) (value >>> 8);
            serialized[offset++] = (byte) (value >>> 16);
        }
        return offset;
    }

    private static long readLongValue( byte[] serialized, int offset, int size )
    {
        if ( size == 3 )
        {
            return unsigned( serialized[offset] ) |
                    (unsignedLong( serialized[offset + 1] ) << 8) |
                    (unsignedLong( serialized[offset + 2] ) << 16) |
                    (unsignedLong( serialized[offset + 3] ) << 24) |
                    (unsignedLong( serialized[offset + 4] ) << 32) |
                    (unsignedLong( serialized[offset + 5] ) << 40) |
                    (unsignedLong( serialized[offset + 6] ) << 48);
        }
        else if ( size == 2 )
        {
            return unsignedLong( serialized[offset] ) |
                    (unsignedLong( serialized[offset + 1] ) << 8) |
                    (unsignedLong( serialized[offset + 2] ) << 16) |
                    (unsignedLong( serialized[offset + 3] ) << 24) |
                    (unsignedLong( serialized[offset + 4] ) << 32);
        }
        else if ( size == 1 )
        {
            return unsignedLong( serialized[offset] ) |
                    (unsignedLong( serialized[offset + 1] ) << 8) |
                    (unsignedLong( serialized[offset + 2] ) << 16) |
                    (unsignedLong( serialized[offset + 3] ) << 24);
        }
        return unsignedLong( serialized[offset] ) |
                (unsignedLong( serialized[offset + 1] ) << 8) |
                (unsignedLong( serialized[offset + 2] ) << 16);
    }

    private static int unsigned( byte value )
    {
        return value & 0xFF;
    }

    private static long unsignedLong( byte value )
    {
        return value & 0xFF;
    }

    static IntArrayTarget intArrayTarget()
    {
        return new IntArrayTarget();
    }

    interface Source
    {
        int length();

        int valueAt( int i );
    }

    static class IntArraySource implements Source
    {
        private final int[] array;

        IntArraySource( int[] array )
        {
            this.array = array;
        }

        @Override
        public int length()
        {
            return array.length;
        }

        @Override
        public int valueAt( int i )
        {
            return array[i];
        }
    }

    interface Target
    {
        void beginBlock( int size, int accumulatedSize );

        void accept( int i, int value );
    }

    static class IntArrayTarget implements Target
    {
        private int[] array;

        @Override
        public void beginBlock( int size, int accumulatedSize )
        {
            array = size == accumulatedSize ? new int[size] : Arrays.copyOf( array, accumulatedSize );
        }

        @Override
        public void accept( int i, int value )
        {
            array[i] = value;
        }

        int[] array()
        {
            return array;
        }
    }

    static class LongArrayTarget implements Target
    {
        private long[] array;

        @Override
        public void beginBlock( int size, int accumulatedSize )
        {
            array = size == accumulatedSize ? new long[size] : Arrays.copyOf( array, accumulatedSize );
        }

        @Override
        public void accept( int i, int value )
        {
            array[i] = value;
        }

        long[] array()
        {
            return array;
        }
    }

    static Target SKIP = new Target()
    {
        @Override
        public void beginBlock( int size, int accumulatedSize )
        {
        }

        @Override
        public void accept( int i, int value )
        {
        }
    };
}
