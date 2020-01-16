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

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.util.Preconditions;

class Record
{
    static int FLAG_IN_USE = 0x8;

    public static final int SIZE_BASE = 64;

    // not stored
    private int sizeMultiple;
    long id;

    // stored header
    byte flags;

    // stored data
    // TODO this could be off-heap or something less garbagy
    private ByteBuffer data;
    private boolean dataIsFromShared;

    // temporary abstraction when trying out writes
    MutableNodeRecordData node;

    Record( int sizeMultiple )
    {
        this( sizeMultiple, -1 );
    }

    Record( int sizeMultiple, long internalId )
    {
        this.sizeMultiple = sizeMultiple;
        id = internalId;
    }

    private void createNewDataBuffer( int sizeMultiple )
    {
        data = ByteBuffer.wrap( new byte[sizeMultiple * SIZE_BASE] );
        this.sizeMultiple = sizeMultiple;
    }

    ByteBuffer dataForReading()
    {
        return data;
    }

    ByteBuffer dataForWriting()
    {
        Preconditions.checkState( !dataIsFromShared, "Probably not wanna write with this one" );
        if ( data == null )
        {
            createNewDataBuffer( sizeMultiple );
        }
        return data;
    }

    void initializeFromWithSharedData( Record record )
    {
        this.id = record.id;
        this.flags = record.flags;
        this.data = record.data.duplicate();
        this.data.position( 0 );
        this.dataIsFromShared = true;
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
        return (byte) sizeMultiple;
    }

    void serialize( WritableChannel channel ) throws IOException
    {
        channel.put( (byte) (flags | sizeMultiple()) );
        if ( hasFlag( FLAG_IN_USE ) )
        {
            node.serialize( dataForWriting() );
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
        cursor.putByte( (byte) (flags | sizeMultiple()) );
        cursor.putBytes( data.array(), 0, length );
    }

    void clear()
    {
        flags = 0;
        if ( dataIsFromShared )
        {
            data = null;
            dataIsFromShared = false;
        }
        else if ( data != null )
        {
            data.clear();
        }
    }

    void deserialize( PageCursor cursor )
    {
        int flagsRaw = cursor.getByte() & 0xFF;
        int sizeMultiple = flagsRaw & 0b11;
        int length = sizeMultiple * SIZE_BASE;
        flags = (byte) (flagsRaw & 0b1111_1100);
        if ( length > cursor.getCurrentPageSize() || length <= 0 )
        {
            cursor.setCursorException( "Invalid length " + length );
            return;
        }
        if ( data == null || length > data.capacity() )
        {
            createNewDataBuffer( sizeMultiple );
        }
        cursor.getBytes( data.array(), 0, length );
    }
}
