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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.pagecache.PageCursor;

class Record
{
    static int FLAG_IN_USE = 0x8;

    public static final int SIZE_BASE = 64;
    public static final int HEADER_SIZE = 1;

    // not stored
    private int sizeExp;
    long id;

    // stored header
    byte flags;

    // stored data
    // TODO this could be off-heap or something less garbagy
    private ByteBuffer data;

    Record( int sizeExp )
    {
        this( sizeExp, -1 );
    }

    Record( int sizeExp, long id )
    {
        this.sizeExp = sizeExp;
        this.id = id;
    }

    static int recordSize( int sizeExp )
    {
        return SIZE_BASE << sizeExp;
    }

    private void createNewDataBuffer( int sizeExp )
    {
        data = ByteBuffer.wrap( new byte[recordSize( sizeExp ) - HEADER_SIZE] );
        this.sizeExp = sizeExp;
    }

    ByteBuffer dataForReading()
    {
        return data;
    }

    ByteBuffer dataForWriting()
    {
        if ( data == null )
        {
            createNewDataBuffer( sizeExp );
        }
        return data;
    }

    void initializeFromSharedData( Record record )
    {
        this.id = record.id;
        this.flags = record.flags;
        if ( data == null || data.capacity() < record.data.capacity() )
        {
            createNewDataBuffer( record.sizeExp );
        }
        else
        {
            data.clear();
        }
        System.arraycopy( record.data.array(), 0, data.array(), 0, record.data.capacity() );
    }

    void setFlag( int flag, boolean value )
    {
        if ( value )
        {
            flags |= flag;
        }
        else
        {
            flags &= ~flag;
        }
    }

    boolean hasFlag( int flag )
    {
        return (flags & flag) == flag;
    }

    byte sizeExp()
    {
        return (byte) sizeExp;
    }

    void serialize( WritableChannel channel ) throws IOException
    {
        channel.put( (byte) (flags | sizeExp()) );
        if ( hasFlag( FLAG_IN_USE ) )
        {
            int length = data.position();
            // write the length so that we save on tx-log command size
            channel.putShort( (short) length );
            channel.put( data.array(), length );
        }
    }

    // === UNIFY THESE SOMEHOW LATER ===

    void serialize( PageCursor cursor )
    {
        int length = data.position();
        cursor.putByte( (byte) (flags | sizeExp()) );
        cursor.putBytes( data.array(), 0, length );
    }

    void clear()
    {
        flags = 0;
        if ( data != null )
        {
            data.clear();
        }
    }

    void loadRecord( PageCursor cursor, int offset ) throws IOException
    {
        // First read the header byte in its own shouldRetry-loop because how we read the data depends on this
        int flagsRaw = safelyReadHeader( cursor, offset );
        int sizeExp = flagsRaw & 0b111;
        int length = recordSize( sizeExp ) - HEADER_SIZE;
        flags = (byte) (flagsRaw & 0b1111_1000);
        if ( length > cursor.getCurrentPageSize() || length <= 0 )
        {
            cursor.setCursorException( "Invalid length " + length );
            return;
        }
        if ( data == null || length > data.capacity() )
        {
            createNewDataBuffer( sizeExp );
        }

        do
        {
            cursor.setOffset( offset + HEADER_SIZE );
            cursor.getBytes( data.array(), 0, length );
        }
        while ( cursor.shouldRetry() );
    }

    private static int safelyReadHeader( PageCursor cursor, int offset ) throws IOException
    {
        int flagsRaw;
        do
        {
            flagsRaw = cursor.getByte( offset ) & 0xFF;
        }
        while ( cursor.shouldRetry() );
        return flagsRaw;
    }

    static boolean isInUse( PageCursor cursor, int offset ) throws IOException
    {
        int flagsRaw = safelyReadHeader( cursor, offset );
        return (flagsRaw & FLAG_IN_USE) != 0;
    }

    void copyContentsFrom( Record source )
    {
        id = source.id;
        flags = source.flags;
        sizeExp = source.sizeExp;
        if ( source.data != null )
        {
            createNewDataBuffer( sizeExp );
            System.arraycopy( source.data.array(), 0, data.array(), 0, source.data.capacity() );
        }
        else
        {
            data = null;
        }
    }

    @Override
    public String toString()
    {
        return "Record{" + "sizeExp=" + sizeExp + ", id=" + id + ", flags=" + flags + ", data=" +
               (data != null ? Arrays.toString( Arrays.copyOf( data.array(), data.position() ) ) : "[]") + '}';
    }
}
