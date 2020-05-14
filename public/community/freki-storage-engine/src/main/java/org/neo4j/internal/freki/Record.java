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

<<<<<<< HEAD
=======
import static java.lang.String.format;

>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
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

    static final int SIZE_BASE = 128;
    static final int HEADER_SIZE = 1;

    // not stored
    long id;
    private int dataLength;

    // stored
    byte flags;
    private final ByteBuffer data;

    Record( int sizeExp )
    {
        this( sizeExp, -1 );
    }

    Record( int sizeExp, long id )
    {
<<<<<<< HEAD
        this( sizeExpAsFlagsByte( sizeExp ), id );
    }

    private Record( byte flags, long id )
=======
        this( sizeExpAsFlagsByte( sizeExp ), id, true );
    }

    private Record( byte flags, long id, boolean instantiateData )
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
    {
        this.flags = flags;
        this.id = id;
        this.dataLength = recordDataSize( sizeExp() );
<<<<<<< HEAD
        this.data = ByteBuffer.wrap( new byte[dataLength] );
=======
        // for instantiatedData == false this is a record which will never be used as anything other than deleting a record
        this.data = instantiateData ? ByteBuffer.wrap( new byte[dataLength] ) : null;
    }

    static Record deletedRecord( int sizeExp, long id )
    {
        return new Record( sizeExpAsFlagsByte( sizeExp ), id, false );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
    }

    static int recordSize( int sizeExp )
    {
        return SIZE_BASE << sizeExp;
    }

    static int recordDataSize( int sizeExp )
    {
        return recordSize( sizeExp ) - HEADER_SIZE;
    }

    static int recordXFactor( int sizeExp )
    {
        return 1 << sizeExp;
    }

    static int sizeExpFromXFactor( int xFactor )
    {
        return Integer.numberOfTrailingZeros( xFactor );
    }

    ByteBuffer data( int position )
    {
        return data.position( position );
    }

    ByteBuffer data()
    {
        return data;
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

    private static byte sizeExpAsFlagsByte( int sizeExp )
    {
        return (byte) sizeExp;
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
<<<<<<< HEAD
            int length = data.position();
=======
            int length = data.limit();
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
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
            assert length <= recordDataSize( record.sizeExp() ); // if incorrect, fail here instead of OOM
            channel.get( record.data( 0 ).array(), length );
<<<<<<< HEAD
=======
            record.data.position( length ).flip();
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        }
        return record;
    }

    // === UNIFY THESE SOMEHOW LATER ===

    void serialize( PageCursor cursor )
    {
        cursor.putByte( flags );
        if ( data != null )
        {
            int length = data.limit();
            cursor.putBytes( data.array(), 0, length );
        }
        else
        {
            assert !hasFlag( FLAG_IN_USE );
        }
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
        if ( dataLength > cursor.getCurrentPageSize() || dataLength <= 0 )
        {
            cursor.setCursorException( "Invalid length " + dataLength );
            return;
        }

        do
        {
            cursor.setOffset( offset );
            flags = cursor.getByte();
            cursor.getBytes( data.array(), 0, dataLength );
            data.position( dataLength );
            data.flip();
        }
        while ( cursor.shouldRetry() );
        cursor.checkAndClearBoundsFlag();
        cursor.checkAndClearCursorException();
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
        assert source.sizeExp() == sizeExp();
        id = source.id;
        flags = source.flags;
        System.arraycopy( source.data.array(), 0, data.array(), 0, source.data.capacity() );
    }

    @Override
    public String toString()
    {
        String dataString;
<<<<<<< HEAD
=======
        int dataLength = 0;
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
        if ( data == null )
        {
            dataString = "<null>";
        }
        else
        {
<<<<<<< HEAD
            int highestNonZeroLimit = findHighestNonZeroLimit();
            int diff = data.limit() - highestNonZeroLimit;
            dataString = diff >= 8 ? Arrays.toString( Arrays.copyOf( data.array(), findHighestNonZeroLimit() ) ) + "..." + diff + " more zeros"
                                   : Arrays.toString( Arrays.copyOf( data.array(), data.limit() ) );
        }
        return "Record{x" + recordXFactor( sizeExp() ) + ", id=" + id + ", flags=" + flags + ", data=" + dataString + '}';
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
=======
            dataString = Arrays.toString( Arrays.copyOf( data.array(), data.limit() ) );
            dataLength = data.limit();
        }
        return format( "Record{x%d(%d)%s,len=%d, %s}", recordXFactor( sizeExp() ), id, hasFlag( FLAG_IN_USE ) ? "" : " UNUSED ", dataLength, dataString );
>>>>>>> 3547c9f99be18ee92915375142e39440b935bcec
    }

    boolean hasSameContentsAs( Record other )
    {
        return other.id == id && other.flags == flags &&
                ((data == null && other.data == null) || Arrays.equals( other.data.array(), data.array() ));
    }
}
