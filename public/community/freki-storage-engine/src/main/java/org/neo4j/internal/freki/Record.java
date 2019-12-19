/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.pagecache.PageCursor;

class Record
{
    static int FLAG_IN_USE = 0x1;

    public static final int SIZE_BASE = 64;

    transient long internalId; // transient just to remember to not store it, it's the location to store it at
    byte flags;
    long externalId;
    long txId;
    long txPrev;
    long prev;
    long next;

    // TODO this could be off-heap or something less garbagy
    ByteBuffer data;

    Record( int sizeMultiple )
    {
        this( sizeMultiple, -1 );
    }

    Record( int sizeMultiple, long internalId )
    {
        data = ByteBuffer.wrap( new byte[sizeMultiple * SIZE_BASE] );
        this.internalId = internalId;
    }

    boolean hasFlag( int flag )
    {
        return (flags & flag) == flag;
    }

    void serialize( WritableChannel channel ) throws IOException
    {
        channel.put( flags );
        channel.putLong( externalId );
        channel.putLong( txId );
        channel.putLong( txPrev );
        channel.putLong( prev );
        channel.putLong( next );
        channel.putInt( data.limit() );
        channel.put( data.array(), data.limit() );
    }

    // === UNIFY THESE SOMEHOW LATER ===

    void serialize( PageCursor cursor )
    {
        cursor.putByte( flags );
        cursor.putLong( externalId );
        cursor.putLong( txId );
        cursor.putLong( txPrev );
        cursor.putLong( prev );
        cursor.putLong( next );
        cursor.putInt( data.limit() );
        cursor.putBytes( data.array(), 0, data.limit() );
    }

    void clear()
    {
        internalId = -1;
        flags = 0;
        externalId = -1;
        txId = -1;
        txPrev = -1;
        prev = -1;
        next = -1;
        data.clear();
    }

    void deserialize( PageCursor cursor )
    {
        flags = cursor.getByte();
        externalId = cursor.getLong();
        txId = cursor.getLong();
        txPrev = cursor.getLong();
        prev = cursor.getLong();
        next = cursor.getLong();
        int length = cursor.getInt();
        if ( length > cursor.getCurrentPageSize() || length < 0 )
        {
            cursor.setCursorException( "Invalid length " + length );
            return;
        }
        if ( length > data.capacity() )
        {
            data = ByteBuffer.wrap( new byte[length] );
        }
        cursor.getBytes( data.array(), 0, length );
    }
}
