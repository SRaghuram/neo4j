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

import org.eclipse.collections.api.iterator.LongIterator;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;

class StreamVByte
{
    private static final int[] INT_RELATIVE_OFFSETS = new int[256];
    private static final int[] LONG_RELATIVE_OFFSETS = new int[256];
    private static final int[] INT_CODE_SIZES = {1, 2, 3, 4};
    private static final int[] LONG_CODE_SIZES = {1, 3, 5, 7};
    private static final long LONG_BYTE_SIZE_0 = 1L << (LONG_CODE_SIZES[0] * Byte.SIZE);
    private static final long LONG_BYTE_SIZE_1 = 1L << (LONG_CODE_SIZES[1] * Byte.SIZE);
    private static final long LONG_BYTE_SIZE_2 = 1L << (LONG_CODE_SIZES[2] * Byte.SIZE);
    private static final long LONG_BYTE_SIZE_3 = 1L << (LONG_CODE_SIZES[3] * Byte.SIZE);
    static final int SINGLE_VLONG_MAX_SIZE = LONG_CODE_SIZES[LONG_CODE_SIZES.length - 1] + 1;
    static final int DUAL_VLONG_MAX_SIZE = LONG_CODE_SIZES[LONG_CODE_SIZES.length - 1] * 2 + 1;

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

    static int writeInts( int[] values, ByteBuffer buffer, boolean deltas )
    {
        Writer writer = new Writer(); // TODO pass in instead
        writer.initialize( buffer, deltas, INT_CODE_SIZES, INT_ENCODER, values.length );
        for ( int value : values )
        {
            if ( !writer.writeNext( value ) )
            {
                throw new BufferOverflowException();
            }
        }
        writer.done();
        return buffer.position();
    }

    static void writeInts( Writer writer, ByteBuffer buffer, boolean deltas, int worstCaseNumValues )
    {
        writer.initialize( buffer, deltas, INT_CODE_SIZES, INT_ENCODER, worstCaseNumValues );
    }

    static int[] readInts( ByteBuffer buffer, boolean deltas )
    {
        return (int[]) readInts( buffer, deltas, INT_CREATOR, INT_CONSUMER );
    }

    static Object readInts( ByteBuffer buffer, boolean deltas, StreamVByte.TargetCreator creator, StreamVByte.TargetConsumer consumer )
    {
        Reader reader = new Reader();
        reader.initialize( buffer, deltas, INT_CODE_SIZES, INT_DECODER );
        Object values = creator.create( reader.count );
        for ( int i = 0; i < reader.count; i++ )
        {
            consumer.accept( values, (int) reader.next(), i );
        }
        buffer.position( reader.offset );
        return values;
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

    private static byte intValueSizeCode( int value )
    {
        byte code;
        if ( value < (1 << 8) )
        {   // 1 byte
            code = 0;
        }
        else if ( value < (1 << 16) )
        {   // 2 bytes
            code = 1;
        }
        else if ( value < (1 << 24) )
        {   // 3 bytes
            code = 2;
        }
        else
        {   // 4 bytes
            code = 3;
        }
        return code;
    }

    private static void encodeIntValue( int value, byte sizeCode, byte[] bytes, int offset )
    {
        if ( sizeCode == 0 )
        {   // 1 byte
            bytes[offset] = (byte) value;
        }
        else if ( sizeCode == 1 )
        {   // 2 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
        }
        else if ( sizeCode == 2 )
        {   // 3 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
        }
        else
        {   // 4 bytes
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
            bytes[offset + 3] = (byte) (value >> 24);
        }
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

    static void writeLongs( Writer writer, ByteBuffer buffer, int worstCaseNumValues )
    {
        writer.initialize( buffer, false, LONG_CODE_SIZES, LONG_ENCODER, worstCaseNumValues );
    }

    static int writeLongs( long[] values, ByteBuffer buffer )
    {
        Writer writer = new Writer(); // TODO pass in instead
        writeLongs( writer, buffer, values.length );
        for ( long value : values )
        {
            if ( !writer.writeNext( value ) )
            {
                throw new BufferOverflowException();
            }
        }
        writer.done();
        return buffer.position();
    }

    static long[] readLongs( ByteBuffer buffer )
    {
        Reader reader = new Reader();
        reader.initialize( buffer, false, LONG_CODE_SIZES, LONG_DECODER );
        long[] values = new long[reader.count];
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = reader.next();
        }
        buffer.position( reader.offset );
        return values;
    }

    static void readLongs( ByteBuffer buffer, Reader reader )
    {
        reader.initialize( buffer, false, LONG_CODE_SIZES, LONG_DECODER );
    }

    static int sizeOfLongSizeIndex( int code )
    {
        return LONG_CODE_SIZES[code];
    }

    static byte longValueSizeCode( long value )
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

    private static void encodeLongValue( long value, byte sizeCode, byte[] bytes, int offset )
    {
        if ( sizeCode == 0 )
        {
            bytes[offset] = (byte) value;
        }
        else if ( sizeCode == 1 )
        {
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
        }
        else if ( sizeCode == 2 )
        {
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
            bytes[offset + 3] = (byte) (value >> 24);
            bytes[offset + 4] = (byte) (value >> 32);
        }
        else
        {
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >> 8);
            bytes[offset + 2] = (byte) (value >> 16);
            bytes[offset + 3] = (byte) (value >> 24);
            bytes[offset + 4] = (byte) (value >> 32);
            bytes[offset + 5] = (byte) (value >> 40);
            bytes[offset + 6] = (byte) (value >> 48);
        }
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

    private static int figureOutCountHeaderSize( int count )
    {
        assert count <= 0x3FFF;
        if ( count <= 2 )
        {   // doesn't use an additional byte, instead shares with the single header byte
            return 0;
        }
        return count <= 63
               ? 1  // count fits in 1B
               : 2; // count fits in 2B
    }

    private static void writeCountHeader( int count, byte[] bytes, int offset, int style, boolean additive )
    {
        assert count <= 0x3FFF;
        if ( style == 0 )
        {
            // [1_cc,bbaa]
            byte countAndMark = (byte) ((count << 4) | 0x80);
            byte existingHeaderMarks = (byte) (additive ? bytes[offset] & 0xF : 0);
            bytes[offset] = (byte) (existingHeaderMarks | countAndMark);
        }
        else
        {
            bytes[offset++] = (byte) (count & 0x3F);
            if ( style == 2 )
            {
                // [_Mcc,cccc][cccc,cccc]
                bytes[offset - 1] |= 0x40;
                bytes[offset] = (byte) (count >>> 6);
            }
            // else [__cc,cccc]
        }
    }

    private static int readCountHeader( byte[] bytes, int offset )
    {
        byte headerByte = bytes[offset];
        if ( (headerByte & 0x80) != 0 ) // Count is 0-2
        {
            return (headerByte >>> 4) & 0x3;
        }
        else if ( (headerByte & 0x40) == 0 ) // Count is 3-63
        {
            return headerByte | (1 << 16);
        }
        else // Count is 64-16383, so one more byte is required to hold the count
        {
            int count = (headerByte & 0x3F) | (bytes[offset + 1] & 0xFF) << 6;
            return count | (2 << 16);
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

    static class Writer
    {
        // sort-of-final stuff
        private byte[] data;
        private boolean deltas;
        private int[] codeSizes;
        private Encoder encoder;
        private int countHeaderSize;
        private int countHeaderOffset;

        // state while writing
        private long prevValue;
        private int headerOffset;
        private int offset;
        private int count;
        private byte header;
        private int headerShift;
        private ByteBuffer buffer;
        private int limit;

        private void initialize( ByteBuffer buffer, boolean deltas, int[] codeSizes, Encoder encoder, int worstCaseCount )
        {
            this.buffer = buffer;
            this.data = buffer.array();
            this.deltas = deltas;
            this.codeSizes = codeSizes;
            this.encoder = encoder;
            this.prevValue = 0;
            this.count = 0;
            this.countHeaderOffset = buffer.position();
            this.countHeaderSize = figureOutCountHeaderSize( worstCaseCount );
            this.limit = buffer.limit();
//            assert buffer.limit() <= buffer.capacity() - codeSizes[codeSizes.length - 1] :
//                    "Please set proper limit to avoid exceptions in serialization " + buffer;

            writeCountHeader( worstCaseCount, data, countHeaderOffset, countHeaderSize, false );
            this.header = 0;
            this.headerShift = 8;
            this.offset = countHeaderOffset + countHeaderSize;
        }

        /**
         * @return {@code true} if the value was written and fit in the target data array, otherwise {@code false} and the state is left
         * as it was prior to this call.
         */
        boolean writeNext( long value )
        {
            if ( headerShift == 8 )
            {
                if ( count > 0 )
                {
                    data[headerOffset] = header;
                    header = 0;
                }
                if ( offset + 1 >= limit )
                {
                    return false;
                }
                headerOffset = offset++;
                headerShift = 0;
            }

            long valueToWrite = value - prevValue;
            byte sizeCode = encoder.sizeCodeOf( valueToWrite );
            int valueSize = codeSizes[sizeCode];
            if ( offset + valueSize >= limit )
            {
                return false;
            }

            encoder.encodeNext( data, offset, valueToWrite, sizeCode );
            header |= sizeCode << headerShift;
            offset += valueSize;
            if ( deltas )
            {
                prevValue = value;
            }
            headerShift += 2;
            count++;
            return true;
        }

        void undoLastWrite()
        {
            assert count > 0;
            headerShift -= 2;
            int sizeCode = (header >>> headerShift) & 0x3;
            header &= ~(0x3 << headerShift);
            offset -= codeSizes[sizeCode];
            if ( headerShift == 0 )
            {
                // The last value required a new header byte to be written. Undo that too.
                offset--;
            }
            count--;
        }

        /**
         * @return writes any pending headers and returns the offset that would be the next writable offset after the data written by this writer.
         */
        void done()
        {
            if ( headerShift > 0 && count > 0 )
            {
                data[headerOffset] = header;
            }
            if ( count == 0 && countHeaderSize == 0 )
            {
                // Special case because count header and single header byte shares the same byte
                offset++;
            }
            writeCountHeader( count, data, countHeaderOffset, countHeaderSize, true );
            buffer.position( offset );
        }
    }

    private interface Encoder
    {
        byte sizeCodeOf( long value );

        void encodeNext( byte[] data, int offset, long value, byte sizeCode );
    }

    private static final Encoder INT_ENCODER = new Encoder()
    {
        @Override
        public byte sizeCodeOf( long value )
        {
            return intValueSizeCode( (int) value );
        }

        @Override
        public void encodeNext( byte[] data, int offset, long value, byte sizeCode )
        {
            encodeIntValue( (int) value, sizeCode, data, offset );
        }
    };

    private static final Encoder LONG_ENCODER = new Encoder()
    {
        @Override
        public byte sizeCodeOf( long value )
        {
            return longValueSizeCode( value );
        }

        @Override
        public void encodeNext( byte[] data, int offset, long value, byte sizeCode )
        {
            encodeLongValue( value, sizeCode, data, offset );
        }
    };

    static class Reader implements LongIterator
    {
        // TODO experiment with using the relative offsets and reading 4 by 4 as long as possible, could make use of SIMD instructions better

        private byte[] data;
        private int i;
        private int count;
        private boolean deltas;
        private int[] codeSizes;
        private Decoder decoder;

        private byte header;
        private int headerShift;
        private long prevValue;
        private long current;
        private int offset;
        private ByteBuffer buffer;

        private void initialize( ByteBuffer buffer, boolean deltas, int[] codeSizes, Decoder decoder )
        {
            this.buffer = buffer;
            this.data = buffer.array();
            this.deltas = deltas;
            this.codeSizes = codeSizes;
            this.decoder = decoder;
            this.prevValue = 0;
            this.i = 0;
            this.offset = buffer.position();
            int countAndHeaderSize = readCountHeader( data, offset );
            this.count = countAndHeaderSize & 0xFFFF;
            int countHeaderSize = countAndHeaderSize >>> 16;
            if ( countHeaderSize == 0 )
            {
                this.header = data[offset];
                this.headerShift = 0;
                this.offset = offset + 1;
            }
            else
            {
                this.headerShift = 8;
                this.offset = offset + countHeaderSize;
            }
        }

        @Override
        public long next()
        {
            assert i < count;
            if ( headerShift == 8 )
            {
                header = data[offset++];
                headerShift = 0;
            }
            int sizeCode = (header >> headerShift) & 0x3;
            headerShift += 2;
            current = decoder.decodeNext( sizeCode, data, offset );
            if ( deltas )
            {
                current += prevValue;
                prevValue = current;
            }
            offset += codeSizes[sizeCode];
            i++;
            return current;
        }

        @Override
        public boolean hasNext()
        {
            return i < count;
        }

        long current()
        {
            assert i > 0;
            return current;
        }

        void clear()
        {
            i = 0;
            count = 0;
        }
    }

    private interface Decoder
    {
        long decodeNext( int sizeCode, byte[] data, int offset );
    }

    private static final Decoder INT_DECODER = StreamVByte::decodeIntValue;
    private static final Decoder LONG_DECODER = StreamVByte::decodeLongValue;
}
