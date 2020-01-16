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

import java.util.Arrays;

import static java.lang.Integer.min;

class StreamVByte
{
    private static final int MASK_SQUASHED_BLOCK = 0b1000_0000;
    private static final int SHIFT_SQUASHED_BLOCK_LENGTH = 5;

    static int writeDeltas( int[] source, byte[] serialized, int serializedOffset )
    {
        return writeDeltas( new IntArraySource( source ), serialized, serializedOffset );
    }

    static int writeDeltas( Source source, byte[] serialized, int serializedOffset )
    {
        int offset = serializedOffset;
        int length = source.length();
        for ( int i = 0, prev = 0; i < length; )
        {
            int headerOffset = offset;
            int currentBlockValueLength = min( Byte.MAX_VALUE, length - i );
            if ( currentBlockValueLength <= 2 )
            {
                // If block size is 0..2 then count and header bytes can be squashed into a single byte
                serialized[headerOffset] = (byte) (MASK_SQUASHED_BLOCK | (currentBlockValueLength << SHIFT_SQUASHED_BLOCK_LENGTH));
            }
            else
            {
                serialized[headerOffset++] = (byte) currentBlockValueLength;
            }

            offset = headerOffset + 1;
            for ( int c = 0; c < currentBlockValueLength; )
            {
                int blockSize = min( 4, currentBlockValueLength - c );
                for ( int j = 0; j < blockSize; j++, i++, c++ )
                {
                    int value = source.valueAt( i );
                    offset = writeValue( serialized, offset, headerOffset, j, value - prev );
                    prev = value;
                }
                if ( blockSize == 4 )
                {
                    headerOffset = offset++;
                }
            }
        }
        if ( length == 0 )
        {
            serialized[offset++] = (byte) MASK_SQUASHED_BLOCK;
        }
        return offset;
    }

    private static int writeValue( byte[] serialized, int offset, int headerOffset, int j, int value )
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

    static int readDeltas( Target target, byte[] serialized, int serializedOffset )
    {
        int offset = serializedOffset;
        int currentBlockValueLength = Byte.MAX_VALUE;
        for ( int i = 0, prev = 0; currentBlockValueLength == Byte.MAX_VALUE; )
        {
            int headerByte = unsigned( serialized[offset++] );
            if ( (headerByte & MASK_SQUASHED_BLOCK) != 0 )
            {
                // The special 0-2 header and count squashed byte
                currentBlockValueLength = ((headerByte & 0b0110_0000) >>> SHIFT_SQUASHED_BLOCK_LENGTH) & 0b11;
                headerByte &= 0b1111;
            }
            else
            {
                currentBlockValueLength = headerByte;
                headerByte = unsigned( serialized[offset++] );
            }

            target.beginBlock( currentBlockValueLength, i + currentBlockValueLength );
            for ( int c = 0; c < currentBlockValueLength; )
            {
                int blockSize = min( 4, currentBlockValueLength - c );
                for ( int j = 0; j < blockSize; j++, i++, c++ )
                {
                    int size = (headerByte >>> (j * 2)) & 0b11;
                    int readValue = readValue( serialized, offset, size );
                    int value = prev + readValue;
                    target.accept( i, value );
                    offset += size + 1; // because e.g. size==0 uses 1B, size==1 uses 2B a.s.o.
                    prev = value;
                }
                if ( blockSize == 4 )
                {
                    headerByte = unsigned( serialized[offset++] );
                }
            }
        }
        return offset;
    }

    private static int readValue( byte[] serialized, int offset, int size )
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

    private static int unsigned( byte value )
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
