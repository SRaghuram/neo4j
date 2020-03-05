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

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.pagecache.PageCursor;

class Record
{
    /*
      Header 1B
     [____,____]
      │││| |└└└─ SizeExp (3b)
      │││| └──── InUse (1b)
      └└└└────── Unused (4b)
     */
    static int MASK_SIZE_EXP = 0x7;
    static int FLAG_IN_USE = 0x8;

    static final int SIZE_BASE = 64;
    static final int HEADER_SIZE = 1;

    // not stored
    long id;

    // stored
    byte flags;
    private ByteBuffer data;

    Record( int sizeExp )
    {
        this( sizeExp, -1 );
    }

    Record( int sizeExp, long id )
    {
        setSizeExp( sizeExp );
        this.id = id;
    }

    Record( byte flags, long id )
    {
        this.flags = flags;
        this.id = id;
    }

    static int recordSize( int sizeExp )
    {
        return SIZE_BASE << sizeExp;
    }

    static int recordXFactor( int sizeExp )
    {
        return 1 << sizeExp;
    }

    static int sizeExpFromXFactor( int xFactor )
    {
        return Integer.numberOfTrailingZeros( xFactor );
    }

    private void createNewDataBuffer()
    {
        data = ByteBuffer.wrap( new byte[recordSize( sizeExp() ) - HEADER_SIZE] );
    }

    ByteBuffer dataForReading( int position )
    {
        return data.position( position );
    }

    ByteBuffer dataForReading()
    {
        assert data.position() == 0 : data.position();
        return data;
    }

    ByteBuffer dataForWriting()
    {
        if ( data == null )
        {
            createNewDataBuffer();
        }
        return data;
    }

    void initializeFromSharedData( Record record )
    {
        this.id = record.id;
        this.flags = record.flags;
        if ( data == null || data.capacity() < record.data.capacity() )
        {
            createNewDataBuffer();
        }
        else
        {
            data.clear();
        }
        System.arraycopy( record.data.array(), 0, data.array(), 0, record.data.limit() );
        data.position( record.data.limit() );
        data.flip();
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

    private void setSizeExp( int sizeExp )
    {
        flags = (byte) ((flags & ~MASK_SIZE_EXP) | sizeExp);
    }

    byte sizeExp()
    {
        return sizeExp( flags );
    }

    static byte sizeExp( byte flags )
    {
        return (byte) (flags & MASK_SIZE_EXP);
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

    static Record deserialize( ReadableChannel channel, long id ) throws IOException
    {
        byte flags = channel.get();
        Record record = new Record( flags, id );
        if ( record.hasFlag( FLAG_IN_USE ) )
        {
            short length = channel.getShort();
            assert length <= recordSize( record.sizeExp() ) - HEADER_SIZE; // if incorrect, fail here instead of OOM
            ByteBuffer data = record.dataForWriting();
            channel.get( data.array(), length );
        }
        return record;
    }

    // === UNIFY THESE SOMEHOW LATER ===

    void serialize( PageCursor cursor )
    {
        int length = data.limit();
        cursor.putByte( flags );
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
        flags = safelyReadFlags( cursor, offset );
        int sizeExp = sizeExp( flags );
        int length = recordSize( sizeExp ) - HEADER_SIZE;
        if ( length > cursor.getCurrentPageSize() || length <= 0 )
        {
            cursor.setCursorException( "Invalid length " + length );
            return;
        }
        if ( data == null || length > data.capacity() )
        {
            createNewDataBuffer();
        }

        do
        {
            cursor.setOffset( offset + HEADER_SIZE );
            cursor.getBytes( data.array(), 0, length );
            data.position( length );
            data.flip();
        }
        while ( cursor.shouldRetry() );
    }

    private static byte safelyReadFlags( PageCursor cursor, int offset ) throws IOException
    {
        byte flags;
        do
        {
            flags = cursor.getByte( offset );
        }
        while ( cursor.shouldRetry() );
        return flags;
    }

    static boolean isInUse( PageCursor cursor, int offset ) throws IOException
    {
        int flagsRaw = safelyReadFlags( cursor, offset );
        return (flagsRaw & FLAG_IN_USE) != 0;
    }

    void copyContentsFrom( Record source )
    {
        id = source.id;
        flags = source.flags;
        if ( source.data != null )
        {
            createNewDataBuffer();
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
        String dataString;
        if ( data == null )
        {
            dataString = "<null>";
        }
        else
        {
            int highestNonZeroLimit = findHighestNonZeroLimit();
            int diff = data.limit() - highestNonZeroLimit;
            dataString = diff >= 8 ? Arrays.toString( Arrays.copyOf( data.array(), findHighestNonZeroLimit() ) ) + "..." + diff + " more zeros"
                                   : Arrays.toString( Arrays.copyOf( data.array(), data.limit() ) );
        }
        return "Record{" + "id=" + id + ", flags=" + flags + ", data=" + dataString + '}';
    }

    private int findHighestNonZeroLimit()
    {
        int nonZeroLimit = data.limit();
        while ( nonZeroLimit > 0 )
        {
            if ( data.array()[nonZeroLimit - 1] != 0 )
            {
                break;
            }
            nonZeroLimit--;
        }
        return nonZeroLimit;
    }

    boolean hasSameContentsAs( Record other )
    {
        return other.id == id && other.flags == flags &&
                ((data == null && other.data == null) || Arrays.equals( other.data.array(), data.array() ));
    }
}
