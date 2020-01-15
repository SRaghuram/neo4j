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
    static int FLAG_IN_USE = 0x8;

    public static final int SIZE_BASE = 64;

    // not stored
    long id;

    // stored header
    byte flags;

    // stored data
    // TODO this could be off-heap or something less garbagy
    ByteBuffer data;

    // temporary abstraction when trying out writes
    MutableNodeRecordData node;

    Record( int sizeMultiple )
    {
        this( sizeMultiple, -1 );
    }

    Record( int sizeMultiple, long internalId )
    {
        id = internalId;
        data = ByteBuffer.wrap( new byte[sizeMultiple * SIZE_BASE] );
    }

    void setFlag( int flag )
    {
        flags |= flag;
    }

    boolean hasFlag( int flag )
    {
        return (flags & flag) == flag;
    }

    private byte sizeMultiple()
    {
        return (byte) (data.capacity() / SIZE_BASE);
    }

    void serialize( WritableChannel channel ) throws IOException
    {
        node.serialize( data );
        int length = data.position();
        channel.put( (byte) (flags | sizeMultiple()) );
        // write the length so that we save on tx-log command size
        channel.putShort( (short) length );
        channel.put( data.array(), length );
    }

    // === UNIFY THESE SOMEHOW LATER ===

    void serialize( PageCursor cursor )
    {
        int length = data.position();
        cursor.putByte( (byte) (flags | sizeMultiple()) );
        cursor.putBytes( data.array(), 0, length );
    }

    void clear()
    {
        data.clear();
        flags = 0;
    }

    void deserialize( PageCursor cursor )
    {
        int flagsRaw = cursor.getByte() & 0xFF;
        int length = (flagsRaw & 0b11) * SIZE_BASE;
        flags = (byte) (flagsRaw & 0b1111_1100);
        if ( length > cursor.getCurrentPageSize() || length <= 0 )
        {
            cursor.setCursorException( "Invalid length " + length );
            return;
        }
        if ( length > data.capacity() )
        {
//            data = ByteBuffer.wrap( new byte[length] );
            throw new UnsupportedOperationException( "Not implemented yet" );
        }
        cursor.getBytes( data.array(), 0, length );
    }
}
