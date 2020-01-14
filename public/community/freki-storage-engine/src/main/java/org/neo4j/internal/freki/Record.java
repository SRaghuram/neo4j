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

    long id;

    // TODO this could be off-heap or something less garbagy
    ByteBuffer data;
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

//    boolean hasFlag( int flag )
//    {
//        return (flags & flag) == flag;
//    }

    void serialize( WritableChannel channel ) throws IOException
    {
        node.serialize( data );
        channel.putInt( data.limit() - data.position() );
        int pos = data.position();
        while ( pos < data.limit() )
        {
            channel.put( data.get( pos++ ) );
        }
    }

    // === UNIFY THESE SOMEHOW LATER ===

    void serialize( PageCursor cursor )
    {
        int length = data.limit() - data.position();
        cursor.putInt( length );
        cursor.putBytes( data.array(), data.position(), length );
    }

    void clear()
    {
        data.clear();
    }

    void deserialize( PageCursor cursor )
    {
        int length = cursor.getInt();
        if ( length > cursor.getCurrentPageSize() || length < 0 )
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
