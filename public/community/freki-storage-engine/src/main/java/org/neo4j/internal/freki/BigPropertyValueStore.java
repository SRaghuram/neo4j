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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.util.Preconditions;

import static org.neo4j.internal.freki.StreamVByte.INT_ENCODER;
import static org.neo4j.internal.freki.StreamVByte.LONG_ENCODER;
import static org.neo4j.internal.freki.StreamVByte.decodeIntValue;
import static org.neo4j.internal.freki.StreamVByte.decodeLongValue;

class BigPropertyValueStore extends BareBoneSingleFileStore implements SimpleBigValueStore
{
    // [bbaa,  fl]
    // f: first one
    // l: last one
    // a: length
    // Single record:  [    ,  11] [1B length]
    // First in chain: [1001,  10] [5B Forward pointer] [3B totalLength]
    // Mid in chain:   [10  ,  00] [5B Forward pointer]
    // Last in chain:  [    ,  01] [1B length]

    private static final int RECORD_SIZE = 128;
    static final int RECORD_DATA_SIZE = RECORD_SIZE - Record.HEADER_SIZE;
    private static final int LAST_RECORD_LENGTH_SIZE = 1;
    private static final int HEADER_MARK_LAST = 0b1;
    private static final int HEADER_MARK_FIRST = 0b10;

    BigPropertyValueStore( File file, PageCache pageCache, IdGeneratorFactory idGeneratorFactory, boolean readOnly, boolean createIfNotExists,
            PageCacheTracer pageCacheTracer )
    {
        super( file, pageCache, idGeneratorFactory, IdType.STRING_BLOCK, readOnly, createIfNotExists, RECORD_SIZE, pageCacheTracer );
    }

    @Override
    public void write( PageCursor cursor, Iterable<Record> records ) throws IOException
    {
        for ( Record record : records )
        {
            long id = record.id;
            long page = idPage( id );
            int offset = idOffset( id );
            goToPage( cursor, page );
            cursor.setOffset( offset );
            record.serialize( cursor );
            cursor.checkAndClearBoundsFlag();
        }
    }

    @Override
    public ByteBuffer read( PageCursor cursor, IntFunction<ByteBuffer> bufferCreator, long startId ) throws IOException
    {
        long nextId = startId;
        ByteBuffer buffer = null;
        while ( buffer == null || buffer.hasRemaining() )
        {
            Preconditions.checkState( nextId != -1, "Unexpected end of chain, chain started at %d", startId );
            long page = idPage( nextId );
            int offset = idOffset( nextId );
            goToPage( cursor, page );

            // Read header safely
            byte header;
            int totalLength;
            int recordDataLength;
            boolean isFirst;
            boolean isLast;
            do
            {
                cursor.setOffset( offset );
                header = cursor.getByte();
                isFirst = hasMark( header, HEADER_MARK_FIRST );
                isLast = hasMark( header, HEADER_MARK_LAST );
                totalLength = isFirst ? isLast ? cursor.getByte() & 0xFF : decodeIntValue( cursor, (header >>> 4) & 0x3 ) : -1;
                int nextIdSizeCode = (header >>> 6) & 0x3;
                nextId = isLast ? -1 : decodeLongValue( cursor, nextIdSizeCode );
                recordDataLength = isLast ? isFirst ? totalLength : cursor.getByte() & 0xFF : RECORD_SIZE - (cursor.getOffset() - offset);
            }
            while ( cursor.shouldRetry() );
            cursor.checkAndClearBoundsFlag();
            cursor.checkAndClearCursorException();

            // Read data for this record
            if ( isFirst )
            {
                Preconditions.checkState( buffer == null, "%d is marked as being first in chain, but should not be, chain started at %d", nextId, startId );
                buffer = ByteBuffer.wrap( new byte[totalLength] );
            }
            assert buffer != null;
            int cursorOffset = cursor.getOffset();
            int bufferOffset = buffer.position();
            do
            {
                buffer.position( bufferOffset );
                buffer.limit( bufferOffset + recordDataLength );
                cursor.copyTo( cursorOffset, buffer );
                buffer.limit( buffer.capacity() );
            }
            while ( cursor.shouldRetry() );
        }
        return buffer.flip();
    }

    private static boolean hasMark( byte header, int mark )
    {
        return (header & mark) != 0;
    }

    private void goToPage( PageCursor cursor, long page ) throws IOException
    {
        if ( !cursor.next( page ) )
        {
            throw new IOException( "Couldn't go to page " + page );
        }
    }

    /**
     * Allocates space for the given {@code data}, splitting it up into multiple {@link Record records} if required. The resulting
     * list contains the records with its allocated IDs, ready to be written.
     */
    @Override
    public List<Record> allocate( ByteBuffer data, PageCursorTracer cursorTracer )
    {
        List<Record> records = new ArrayList<>();
        long nextId = idGenerator.nextId( cursorTracer );
        int totalLength = data.remaining();
        boolean isFirst = true;
        while ( data.hasRemaining() )
        {
            long recordId = nextId;
            byte header = (byte) ((isFirst ? HEADER_MARK_FIRST : 0) | Record.FLAG_IN_USE);
            ByteBuffer buffer;
            if ( data.remaining() + LAST_RECORD_LENGTH_SIZE <= RECORD_DATA_SIZE )
            {
                header |= HEADER_MARK_LAST;
                buffer = ByteBuffer.wrap( new byte[data.remaining() + LAST_RECORD_LENGTH_SIZE] );
                buffer.put( (byte) data.remaining() );
                buffer.put( data );
            }
            else
            {
                buffer = ByteBuffer.wrap( new byte[RECORD_DATA_SIZE] );
                // Total length, if first
                if ( isFirst )
                {
                    header |= vByteEncode( INT_ENCODER, totalLength, buffer ) << 4;
                }

                // Forward pointer
                nextId = idGenerator.nextId( cursorTracer );
                header |= vByteEncode( LONG_ENCODER, nextId, buffer ) << 6;

                // Data
                int prevLimit = data.limit();
                data.limit( data.position() + RECORD_DATA_SIZE - buffer.position() );
                buffer.put( data );
                data.limit( prevLimit );
            }
            records.add( new Record( header, recordId, buffer ) );
            isFirst = false;
        }
        return records;
    }

    private byte vByteEncode( StreamVByte.Encoder encoder, long value, ByteBuffer buffer )
    {
        byte sizeCode = encoder.sizeCodeOf( value );
        encoder.encodeNext( buffer.array(), buffer.position(), value, sizeCode );
        buffer.position( buffer.position() + encoder.sizeOf( sizeCode ) );
        return sizeCode;
    }
}
