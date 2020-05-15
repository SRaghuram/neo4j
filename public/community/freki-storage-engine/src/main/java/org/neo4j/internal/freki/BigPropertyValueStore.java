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
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static java.lang.Integer.min;

public class BigPropertyValueStore extends BareBoneSingleFileStore implements SimpleBigValueStore
{
    private static final int LENGTH_SIZE = 4;
    private static final int HEADER_SIZE = Long.BYTES; //for now, only a long in the header

    private final AtomicLong nextPosition = new AtomicLong( HEADER_SIZE );

    public BigPropertyValueStore( File file, PageCache pageCache, boolean readOnly, boolean createIfNotExists )
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
        }
    }

    @Override
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
            }
            while ( cursor.shouldRetry() );
            cursor.checkAndClearBoundsFlag();
            cursor.checkAndClearCursorException();

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
    }

    private void goToPage( PageCursor cursor, long page ) throws IOException
    {
        if ( !cursor.next( page ) )
        {
            throw new IOException( "Couldn't go to page " + page );
        }
    }

    /**
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
    }
}
