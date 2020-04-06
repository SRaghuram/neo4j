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

import org.neo4j.io.pagecache.PageCursor;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

class StreamVByte
{
    private static final int[] INT_CODE_SIZES = {1, 2, 3, 4};
    private static final int[] INT_RELATIVE_OFFSETS = new int[256];
    private static final int[] LONG_CODE_SIZES = {1, 3, 5, 7};
    private static final int[] LONG_RELATIVE_OFFSETS = new int[256];
    private static final long LONG_BYTE_SIZE_0 = 1L << (LONG_CODE_SIZES[0] * Byte.SIZE);
    private static final long LONG_BYTE_SIZE_1 = 1L << (LONG_CODE_SIZES[1] * Byte.SIZE);
    private static final long LONG_BYTE_SIZE_2 = 1L << (LONG_CODE_SIZES[2] * Byte.SIZE);
    private static final long LONG_BYTE_SIZE_3 = 1L << (LONG_CODE_SIZES[3] * Byte.SIZE);

    static
    {
        calculateRelativeOffsets( INT_RELATIVE_OFFSETS, INT_CODE_SIZES );
        calculateRelativeOffsets( LONG_RELATIVE_OFFSETS, LONG_CODE_SIZES );
    }

    private static void calculateRelativeOffsets( int[] target, int[] codeSizes )
    {
        for ( int i = 0; i < 256; i++ )
        {
            int d = i & 0x3;
            int c = (i >>> 2) & 0x3;
            int b = (i >>> 4) & 0x3;
            int a = (i >>> 6) & 0x3;
            target[i] =
                    (codeSizes[a] + codeSizes[b] + codeSizes[c] + codeSizes[d]) << 24 |
                            (codeSizes[b] + codeSizes[c] + codeSizes[d]) << 16 |
                            (codeSizes[c] + codeSizes[d]) << 8 |
                            codeSizes[d];
        }
    }

    // === INTS ===

    private static int writeInts( int[] values, byte[] bytes, int offset, boolean deltas )
    {
        int count = values.length;
        if ( count == 0 )
        {
            bytes[offset] = 0;
            return offset + 1;
        }

        int numberOfHeaderBytes = numHeaderBytes( count );
        byte shift = 0;
        int countHeader = writeCountHeader( count, bytes, offset );
        int headerOffset = countHeader & 0xFFFFFF;
        byte key = (byte) (countHeader >>> 24);
        int dataOffset = headerOffset + numberOfHeaderBytes;
        int prev = 0;
        for ( int c = 0; c < count; c++ )
        {
            if ( shift == 8 )
            {
                shift = 0;
                bytes[headerOffset++] = key;
                key = 0;
            }
            int value = values[c];
            byte code = encodeIntValue( deltas ? value - prev : value, bytes, dataOffset );
            dataOffset += INT_CODE_SIZES[code];
            key |= code << shift;
            shift += 2;
            prev = value;
        }

        bytes[headerOffset] = key;
        return dataOffset;
    }

    static void writeInts( int[] values, ByteBuffer buffer )
    {
        buffer.position( writeInts( values, buffer.array(), buffer.position(), false ) );
    }

    static void writeIntDeltas( int[] values, ByteBuffer buffer )
    {
        buffer.position( writeInts( values, buffer.array(), buffer.position(), true ) );
    }

    static int[] readInts( byte[] bytes, int offset, ByteBuffer buffer )
    {
        if ( bytes[offset] == 0 )
        {
            buffer.position( offset + 1 );
            return EMPTY_INT_ARRAY;
        }
        int countAndHeaderOffset = readCountHeader( bytes, offset );
        int count = countAndHeaderOffset & 0xFFFF;
        int headerOffset = countAndHeaderOffset >>> 16;
        int[] values = new int[count];
        int numberOfHeaderBytes = numHeaderBytes( count );
        int dataOffset = headerOffset + numberOfHeaderBytes;
        int valueIndex = 0;
        for ( ; count >= 4; count -= 4 )
        {
            int keyBytes = bytes[headerOffset++] & 0xFF;
            int relativeOffsets = INT_RELATIVE_OFFSETS[keyBytes & 0xFF];
            values[valueIndex] = decodeIntValue( keyBytes & 0x3, bytes, dataOffset );
            values[valueIndex + 1] = decodeIntValue( (keyBytes >>> 2) & 0x3, bytes, dataOffset + (relativeOffsets & 0xFF) );
            values[valueIndex + 2] = decodeIntValue( (keyBytes >>> 4) & 0x3, bytes, dataOffset + ((relativeOffsets >>> 8) & 0xFF) );
            values[valueIndex + 3] = decodeIntValue( (keyBytes >>> 6) & 0x3, bytes, dataOffset + ((relativeOffsets >>> 16) & 0xFF) );
            dataOffset += relativeOffsets >>> 24;
            valueIndex += 4;
        }
        if ( count > 0 )
        {
            int keyBytes = bytes[headerOffset] & 0xFF;
            for ( int i = 0; i < count; i++ )
            {
                int code = (keyBytes >>> (i * 2)) & 0x3;
                values[valueIndex + i] = decodeIntValue( code, bytes, dataOffset );
                dataOffset += INT_CODE_SIZES[code];
            }
        }
        buffer.position( dataOffset );
        return values;
    }

    static int[] readInts( ByteBuffer buffer )
    {
        return readInts( buffer.array(), buffer.position(), buffer );
    }

    static Object readIntDeltas( byte[] bytes, int offset, ByteBuffer buffer, StreamVByte.TargetCreator creator, StreamVByte.TargetConsumer consumer )
    {
        if ( bytes[offset] == 0 )
        {
            buffer.position( offset + 1 );
            return EMPTY_INT_ARRAY;
        }
        int countAndHeaderOffset = readCountHeader( bytes, offset );
        int count = countAndHeaderOffset & 0xFFFF;
        int headerOffset = countAndHeaderOffset >>> 16;
        Object values = creator.create( count );
        int numberOfHeaderBytes = numHeaderBytes( count );
        int dataOffset = headerOffset + numberOfHeaderBytes;
        int valueIndex = 0;
        int prev = 0;
        for ( ; count >= 4; count -= 4 )
        {
            int keyBytes = bytes[headerOffset++] & 0xFF;
            int relativeOffsets = INT_RELATIVE_OFFSETS[keyBytes & 0xFF];
            int value1 = prev + decodeIntValue( keyBytes & 0x3, bytes, dataOffset );
            int value2 = decodeIntValue( (keyBytes >>> 2) & 0x3, bytes, dataOffset + (relativeOffsets & 0xFF) ) + value1;
            int value3 = decodeIntValue( (keyBytes >>> 4) & 0x3, bytes, dataOffset + ((relativeOffsets >>> 8) & 0xFF) ) + value2;
            int value4 = decodeIntValue( (keyBytes >>> 6) & 0x3, bytes, dataOffset + ((relativeOffsets >>> 16) & 0xFF) ) + value3;
            consumer.accept( values, value1, valueIndex );
            consumer.accept( values, value2, valueIndex + 1 );
            consumer.accept( values, value3, valueIndex + 2 );
            consumer.accept( values, value4, valueIndex + 3 );
            dataOffset += relativeOffsets >>> 24;
            prev = value4;
            valueIndex += 4;
        }
        if ( count > 0 )
        {
            int keyBytes = bytes[headerOffset] & 0xFF;
            for ( int i = 0; i < count; i++ )
            {
                int code = (keyBytes >>> (i * 2)) & 0x3;
                int value = decodeIntValue( code, bytes, dataOffset ) + prev;
                consumer.accept( values, value, valueIndex + i );
                dataOffset += INT_CODE_SIZES[code];
                prev = value;
            }
        }
        buffer.position( dataOffset );
        return values;
    }

    static int[] readIntDeltas( byte[] bytes, int offset, ByteBuffer buffer )
    {
        return (int[]) readIntDeltas( bytes, offset, buffer, INT_CREATOR, INT_CONSUMER );
    }

    static int[] readIntDeltas( ByteBuffer buffer )
    {
        return (int[]) readIntDeltas( buffer.array(), buffer.position(), buffer, INT_CREATOR, INT_CONSUMER );
    }

    static Object readIntDeltas( ByteBuffer buffer, TargetCreator creator, TargetConsumer consumer )
    {
        return readIntDeltas( buffer.array(), buffer.position(), buffer, creator, consumer );
    }

    static boolean hasNonEmptyIntArray( ByteBuffer data )
    {
        byte header = data.get( data.position() );
        if ( (header & 0x80) != 0 )
        {
            return (header & 0b0011_0000) != 0;
        }
        return header > 0;
    }

    private static byte encodeIntValue( int value, byte[] bytes, int offset )
    {
        byte code;
        if ( value < (1 << 8) )
        {   // 1 byte
            bytes[offset] = (byte) value;
            code = 0;
        }
        else if ( value < (1 << 16) )
        {   // 2 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            code = 1;
        }
        else if ( value < (1 << 24) )
        {   // 3 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
            code = 2;
        }
        else
        {   // 4 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
            bytes[offset + 3] = (byte) (value >> 24);
            code = 3;
        }
        return code;
    }

    private static int decodeIntValue( int code, byte[] bytes, int dataOffset )
    {
        int value;
        if ( code == 0 )
        {
            value = bytes[dataOffset] & 0xFF;
        }
        else if ( code == 1 )
        {
            value = bytes[dataOffset] & 0xFF | (bytes[dataOffset + 1] & 0xFF) << 8;
        }
        else if ( code == 2 )
        {
            value = bytes[dataOffset] & 0xFF | (bytes[dataOffset + 1] & 0xFF) << 8 | (bytes[dataOffset + 2] & 0xFF) << 16;
        }
        else
        {
            value = bytes[dataOffset] & 0xFF | (bytes[dataOffset + 1] & 0xFF) << 8 | (bytes[dataOffset + 2] & 0xFF) << 16 |
                    (bytes[dataOffset + 3] & 0xFF) << 24;
        }
        return value;
    }

    // === LONGS ===

    static int writeLongs( long[] values, byte[] bytes, int offset )
    {
        int count = values.length;
        if ( count == 0 )
        {
            bytes[offset] = 0;
            return offset + 1;
        }

        int numberOfHeaderBytes = numHeaderBytes( count );
        byte shift = 0;
        int countHeader = writeCountHeader( count, bytes, offset );
        int headerOffset = countHeader & 0xFFFFFF;
        byte key = (byte) (countHeader >>> 24);
        int dataOffset = headerOffset + numberOfHeaderBytes;
        for ( int c = 0; c < count; c++ )
        {
            if ( shift == 8 )
            {
                shift = 0;
                bytes[headerOffset++] = key;
                key = 0;
            }
            long value = values[c];
            byte code = encodeLongValue( value, bytes, dataOffset );
            dataOffset += LONG_CODE_SIZES[code];
            key |= code << shift;
            shift += 2;
        }

        bytes[headerOffset] = key;
        return dataOffset;
    }

    static void writeLongs( long[] values, ByteBuffer buffer )
    {
        buffer.position( writeLongs( values, buffer.array(), buffer.position() ) );
    }

    static long[] readLongs( byte[] bytes, int offset, ByteBuffer buffer )
    {
        if ( bytes[offset] == 0 )
        {
            buffer.position( offset + 1 );
            return EMPTY_LONG_ARRAY;
        }
        int countAndHeaderOffset = readCountHeader( bytes, offset );
        int count = countAndHeaderOffset & 0xFFFF;
        int headerOffset = countAndHeaderOffset >>> 16;
        long[] values = new long[count];
        int numberOfHeaderBytes = numHeaderBytes( count );
        int dataOffset = headerOffset + numberOfHeaderBytes;
        int valueIndex = 0;
        for ( ; count >= 4; count -= 4 )
        {
            int keyBytes = bytes[headerOffset++] & 0xFF;
            int relativeOffsets = LONG_RELATIVE_OFFSETS[keyBytes & 0xFF];
            values[valueIndex] = decodeLongValue( keyBytes & 0x3, bytes, dataOffset );
            values[valueIndex + 1] = decodeLongValue( (keyBytes >>> 2) & 0x3, bytes, dataOffset + (relativeOffsets & 0xFF) );
            values[valueIndex + 2] = decodeLongValue( (keyBytes >>> 4) & 0x3, bytes, dataOffset + ((relativeOffsets >>> 8) & 0xFF) );
            values[valueIndex + 3] = decodeLongValue( (keyBytes >>> 6) & 0x3, bytes, dataOffset + ((relativeOffsets >>> 16) & 0xFF) );
            dataOffset += relativeOffsets >>> 24;
            valueIndex += 4;
        }
        if ( count > 0 )
        {
            int keyBytes = bytes[headerOffset] & 0xFF;
            for ( int i = 0; i < count; i++ )
            {
                int code = (keyBytes >>> (i * 2)) & 0x3;
                values[valueIndex + i] = decodeLongValue( code, bytes, dataOffset );
                dataOffset += LONG_CODE_SIZES[code];
            }
        }
        buffer.position( dataOffset );
        return values;
    }

    static long[] readLongs( ByteBuffer buffer )
    {
        return readLongs( buffer.array(), buffer.position(), buffer );
    }

    static int calculateLongSizeIndex( long value )
    {
        if ( value < LONG_BYTE_SIZE_0 )
        {
            return 0;
        }
        else if ( value < LONG_BYTE_SIZE_1 )
        {
            return 1;
        }
        else if ( value < LONG_BYTE_SIZE_2 )
        {
            return 2;
        }
        return 3;
    }

    static int sizeOfLongSizeIndex( int code )
    {
        return LONG_CODE_SIZES[code];
    }

    private static byte encodeLongValue( long value, byte[] bytes, int offset )
    {
        byte code;
        if ( value < LONG_BYTE_SIZE_0 )
        {   // 1 byte
            bytes[offset] = (byte) value;
            code = 0;
        }
        else if ( value < LONG_BYTE_SIZE_1 )
        {   // 3 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
            code = 1;
        }
        else if ( value < LONG_BYTE_SIZE_2 )
        {   // 5 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
            bytes[offset + 3] = (byte) (value >> 24);
            bytes[offset + 4] = (byte) (value >> 32);
            code = 2;
        }
        else
        {   // 7 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
            bytes[offset + 3] = (byte) (value >> 24);
            bytes[offset + 4] = (byte) (value >> 32);
            bytes[offset + 5] = (byte) (value >> 40);
            bytes[offset + 6] = (byte) (value >> 48);
            code = 3;
        }
        return code;
    }

    private static long decodeLongValue( int code, byte[] bytes, int dataOffset )
    {
        long value;
        if ( code == 0 )
        {
            value = ulong( bytes[dataOffset] );
        }
        else if ( code == 1 )
        {
            value = ulong( bytes[dataOffset] ) |
                    ulong( bytes[dataOffset + 1] ) << 8 |
                    ulong( bytes[dataOffset + 2] ) << 16;
        }
        else if ( code == 2 )
        {
            value = ulong( bytes[dataOffset] ) |
                    ulong( bytes[dataOffset + 1] ) << 8 |
                    ulong( bytes[dataOffset + 2] ) << 16 |
                    ulong( bytes[dataOffset + 3] ) << 24 |
                    ulong( bytes[dataOffset + 4] ) << 32;
        }
        else
        {
            value = ulong( bytes[dataOffset] ) |
                    ulong( bytes[dataOffset + 1] ) << 8 |
                    ulong( bytes[dataOffset + 2] ) << 16 |
                    ulong( bytes[dataOffset + 3] ) << 24 |
                    ulong( bytes[dataOffset + 4] ) << 32 |
                    ulong( bytes[dataOffset + 5] ) << 40 |
                    ulong( bytes[dataOffset + 6] ) << 48;
        }
        return value;
    }

    static void encodeLongValue( PageCursor cursor, long value )
    {
        if ( value < LONG_BYTE_SIZE_0 )
        {
            cursor.putByte( (byte) value );
        }
        else if ( value < LONG_BYTE_SIZE_1 )
        {
            cursor.putByte( (byte) value );
            cursor.putByte( (byte) (value >>> 8) );
            cursor.putByte( (byte) (value >>> 16) );
        }
        else if ( value < LONG_BYTE_SIZE_2 )
        {
            cursor.putByte( (byte) value );
            cursor.putByte( (byte) (value >>> 8) );
            cursor.putByte( (byte) (value >>> 16) );
            cursor.putByte( (byte) (value >>> 24) );
            cursor.putByte( (byte) (value >>> 32) );
        }
        else
        {
            cursor.putByte( (byte) value );
            cursor.putByte( (byte) (value >>> 8) );
            cursor.putByte( (byte) (value >>> 16) );
            cursor.putByte( (byte) (value >>> 24) );
            cursor.putByte( (byte) (value >>> 32) );
            cursor.putByte( (byte) (value >>> 40) );
            cursor.putByte( (byte) (value >>> 48) );
        }
    }

    static long decodeLongValue( PageCursor cursor, int size )
    {
        if ( size == 0 )
        {
            return ulong( cursor.getByte() );
        }
        if ( size == 1 )
        {
            return ulong( cursor.getByte() ) |
                    (ulong( cursor.getByte() ) << 8) |
                    (ulong( cursor.getByte() ) << 16);
        }
        if ( size == 2 )
        {
            return ulong( cursor.getByte() ) |
                    (ulong( cursor.getByte() ) << 8) |
                    (ulong( cursor.getByte() ) << 16) |
                    (ulong( cursor.getByte() ) << 24) |
                    (ulong( cursor.getByte() ) << 32);
        }
        return ulong( cursor.getByte() ) |
                (ulong( cursor.getByte() ) << 8) |
                (ulong( cursor.getByte() ) << 16) |
                (ulong( cursor.getByte() ) << 24) |
                (ulong( cursor.getByte() ) << 32) |
                (ulong( cursor.getByte() ) << 40) |
                (ulong( cursor.getByte() ) << 48);
    }

    private static long ulong( byte value )
    {
        return value & 0xFF;
    }

    private static int numHeaderBytes( int count )
    {
        return (count + 3) / 4;
    }

    private static int writeCountHeader( int count, byte[] bytes, int offset )
    {
        assert count <= 0x3FFF;
        if ( count <= 2 )
        {
            // [1_cc,bbaa]
            bytes[offset] = (byte) ((count << 4) | 0x80);
            return offset | (bytes[offset] << 24);
        }
        else
        {
            bytes[offset++] = (byte) (count & 0x3F);
            if ( count > 63 )
            {
                // [_Mcc,cccc][cccc,cccc]
                bytes[offset - 1] |= 0x40;
                bytes[offset++] = (byte) (count >>> 6);
            }
            // else [__cc,cccc]
            return offset;
        }
    }

    private static int readCountHeader( byte[] bytes, int offset )
    {
        byte headerByte = bytes[offset];
        if ( (headerByte & 0x80) != 0 ) // Count is 0-2
        {
            return ((headerByte >>> 4) & 0x3) | (offset << 16);
        }
        else if ( (headerByte & 0x40) == 0 ) // Count is 3-63
        {
            return headerByte | ((offset + 1) << 16);
        }
        else // Count is 64-16383, so one more byte is required to hold the count
        {
            int count = (headerByte & 0x3F) | (bytes[offset + 1] & 0xFF) << 6;
            return count | ((offset + 2) << 16);
        }
    }

    /**
     * This creator/consumer thing exists only because labels are stored as ints but transferred as longs in the cursor APIs.
     */
    abstract static class TargetCreator
    {
        abstract Object create( int size );
    }

    static final TargetCreator INT_CREATOR = new TargetCreator()
    {
        @Override
        Object create( int size )
        {
            return new int[size];
        }
    };

    static final TargetCreator LONG_CREATOR = new TargetCreator()
    {
        @Override
        Object create( int size )
        {
            return new long[size];
        }
    };

    abstract static class TargetConsumer
    {
        abstract void accept( Object target, int value, int index );
    }

    static final TargetConsumer INT_CONSUMER = new TargetConsumer()
    {
        @Override
        void accept( Object target, int value, int index )
        {
            ((int[]) target)[index] = value;
        }
    };

    static final TargetConsumer LONG_CONSUMER = new TargetConsumer()
    {
        @Override
        void accept( Object target, int value, int index )
        {
            ((long[]) target)[index] = value;
        }
    };
}
