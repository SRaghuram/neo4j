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
<<<<<<< HEAD
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static java.lang.Integer.min;

class BigPropertyValueStore extends BareBoneSingleFileStore implements SimpleBigValueStore
{
    private static final int LENGTH_SIZE = 4;
    private static final int HEADER_SIZE = Long.BYTES; //for now, only a long in the header

    private final AtomicLong nextPosition = new AtomicLong( HEADER_SIZE );

    BigPropertyValueStore( File file, PageCache pageCache, boolean readOnly, boolean createIfNotExists )
    {
        super( file, pageCache, readOnly, createIfNotExists );
    }

    @Override
    public void init() throws IOException
    {
        super.init();
        tryReadHeader();
    }

    @Override
    public void flush( IOLimiter ioLimiter, PageCursorTracer cursorTracer ) throws IOException
    {
        tryWriteHeader( cursorTracer );
        super.flush( ioLimiter, cursorTracer );
    }

    private void tryReadHeader() throws IOException
    {
        if ( !readOnly ) // header only contains information usable for write
        {
            try ( PageCursor cursor = openReadCursor( PageCursorTracer.NULL ) )
            {
                if ( cursor.next( 0 ) )
                {
                    do
                    {
                        nextPosition.set( cursor.getLong( 0 ) );
                    }
                    while ( cursor.shouldRetry() );
                }
                cursor.checkAndClearBoundsFlag();
            }
        }
    }

    private void tryWriteHeader( PageCursorTracer cursorTracer ) throws IOException
    {
        if ( !readOnly )
        {
            try ( PageCursor cursor = openWriteCursor( cursorTracer ) )
            {
                cursor.next( 0 );
                cursor.putLong( 0, nextPosition.get() );
                cursor.checkAndClearBoundsFlag();
            }
        }
    }

    @Override
    public void write( PageCursor cursor, ByteBuffer buffer, long position ) throws IOException
    {
        int length = buffer.remaining();
        long page = position / pageSize;
        int offset = (int) (position % pageSize);
        goToPage( cursor, page );
        cursor.setOffset( offset );
        cursor.putInt( length );
        while ( buffer.remaining() > 0 )
        {
            int remainingInPage = pageSize - cursor.getOffset();
            int toWriteInThisPage = min( buffer.remaining(), remainingInPage );
            cursor.putBytes( buffer.array(), buffer.position(), toWriteInThisPage );
            buffer.position( buffer.position() + toWriteInThisPage );
            if ( buffer.remaining() > 0 )
            {
                goToNextPage( cursor );
            }
=======
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;

import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.util.Preconditions;

import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
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

    BigPropertyValueStore( File file, PageCache pageCache, IdGeneratorFactory idGeneratorFactory, IdType idType, boolean readOnly, boolean createIfNotExists,
            PageCacheTracer pageCacheTracer )
    {
        super( file, pageCache, idGeneratorFactory, idType, readOnly, createIfNotExists, RECORD_SIZE, pageCacheTracer );
    }

    @Override
    public void write( PageCursor cursor, Iterable<Record> records, IdUpdateListener idUpdateListener, PageCursorTracer cursorTracer ) throws IOException
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
            // Assumption, all writes for used records are creations and for unused records are deletions
            idUpdateListener.markId( idGenerator, id, record.hasFlag( FLAG_IN_USE ), cursorTracer );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        }
    }

    @Override
<<<<<<< HEAD
    public boolean read( PageCursor cursor, ByteBuffer buffer, long position ) throws IOException
    {
        int length = length( cursor, position );
        int offset = (int) (position % pageSize) + LENGTH_SIZE;

        while ( length > 0 )
        {
            int toReadInThisPage;
            do
            {
                int remainingInPage = pageSize - offset;
                toReadInThisPage = min( length, remainingInPage );
                cursor.setOffset( offset );
                cursor.getBytes( buffer.array(), buffer.position(), toReadInThisPage );
=======
    public ByteBuffer read( PageCursor cursor, IntFunction<ByteBuffer> bufferCreator, long startId ) throws IOException
    {
        long nextId = startId;
        ByteBuffer buffer = null;
        while ( buffer == null || buffer.hasRemaining() )
        {
            Preconditions.checkState( nextId != NULL, "Unexpected end of chain, chain started at %d", startId );
            long page = idPage( nextId );
            int offset = idOffset( nextId );
            goToPage( cursor, page );

            // Read header safely
            byte header;
            int totalLength;
            int recordDataLength;
            do
            {
                cursor.setOffset( offset );
                header = cursor.getByte();
                totalLength = readTotalLength( cursor, header );
                nextId = readNextId( cursor, header );
                recordDataLength = isLast( header ) ? isFirst( header ) ? totalLength : cursor.getByte() & 0xFF : RECORD_SIZE - (cursor.getOffset() - offset);
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            }
            while ( cursor.shouldRetry() );
            cursor.checkAndClearBoundsFlag();
            cursor.checkAndClearCursorException();

<<<<<<< HEAD
            offset = 0;
            length -= toReadInThisPage;
            buffer.position( buffer.position() + toReadInThisPage );
            if ( buffer.remaining() > 0 )
            {
                goToNextPage( cursor );
            }
        }
        return true;
    }

    @Override
    public int length( PageCursor cursor, long position ) throws IOException
    {
        long page = position / pageSize;
        int offset = (int) (position % pageSize);
        goToPage( cursor, page );
        int length;
        do
        {
            cursor.setOffset( offset );
            length = cursor.getInt();
        }
        while ( cursor.shouldRetry() );
        return length;
    }

    private void goToNextPage( PageCursor cursor ) throws IOException
    {
        if ( !cursor.next() )
        {
            throw new IOException( "Couldn't go to next page" );
        }
=======
            // Read data for this record
            if ( isFirst( header ) )
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

    private long readNextId( PageCursor cursor, byte header )
    {
        return isLast( header ) ? NULL : decodeLongValue( cursor, (header >>> 6) & 0x3 );
    }

    private int readTotalLength( PageCursor cursor, byte header )
    {
        return isFirst( header ) ? isLast( header ) ? cursor.getByte() & 0xFF : decodeIntValue( cursor, (header >>> 4) & 0x3 ) : -1;
    }

    private static boolean isFirst( byte header )
    {
        return (header & HEADER_MARK_FIRST) != 0;
    }

    private static boolean isLast( byte header )
    {
        return (header & HEADER_MARK_LAST) != 0;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    private void goToPage( PageCursor cursor, long page ) throws IOException
    {
        if ( !cursor.next( page ) )
        {
            throw new IOException( "Couldn't go to page " + page );
        }
    }

    /**
<<<<<<< HEAD
     * Allocates space of the given length and atomically advances the position for the next allocation.
     * This is slightly more complicated than getAndAdd because we write an int in the beginning and it will be messy
     * to write parts of that int in one page and part on the next page, so if there's less than 4 bytes available on the current page
     * it will skip to the next page and start there instead.
     */
    @Override
    public long allocateSpace( int length )
    {
        long expect;
        long pos;
        long update;
        do
        {
            expect = nextPosition.get();
            pos = expect;
            int positionInPage = (int) (expect % pageSize);
            if ( positionInPage > (pageSize - LENGTH_SIZE) )
            {
                int diff = pageSize - positionInPage;
                pos += diff;
            }
            update = pos + length + LENGTH_SIZE;
        }
        while ( !nextPosition.compareAndSet( expect, update ) );
        return pos;
    }

    @Override
    public long position()
    {
        return nextPosition.get();
=======
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
            byte header = (byte) ((isFirst ? HEADER_MARK_FIRST : 0) | FLAG_IN_USE);
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

    @Override
    public void visitRecordChainIds( PageCursor cursor, long id, LongConsumer chainVisitor )
    {
        try
        {
            while ( id != NULL )
            {
                chainVisitor.accept( id );
                long page = idPage( id );
                int offset = idOffset( id );
                goToPage( cursor, page );
                do
                {
                    cursor.setOffset( offset );
                    byte header = cursor.getByte();
                    readTotalLength( cursor, header ); // just get it out of the way, so that we can read nextId
                    id = readNextId( cursor, header );
                }
                while ( cursor.shouldRetry() );
                cursor.checkAndClearBoundsFlag();
                cursor.checkAndClearCursorException();
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private byte vByteEncode( StreamVByte.Encoder encoder, long value, ByteBuffer buffer )
    {
        byte sizeCode = encoder.sizeCodeOf( value );
        encoder.encodeNext( buffer.array(), buffer.position(), value, sizeCode );
        buffer.position( buffer.position() + encoder.sizeOf( sizeCode ) );
        return sizeCode;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }
}
