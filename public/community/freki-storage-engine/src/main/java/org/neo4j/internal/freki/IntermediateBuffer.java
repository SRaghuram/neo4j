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

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import java.nio.ByteBuffer;

class IntermediateBuffer
{
    private static final int LEEWAY_SIZE = 50;

    private final MutableList<ByteBuffer> buffers = Lists.mutable.empty();
    private final int bufferCapacity;
    private int count;
    private int readIndex;
    private ByteBuffer tempBuffer;

    IntermediateBuffer( int bufferCapacity )
    {
        this.bufferCapacity = bufferCapacity;
    }

    ByteBuffer add()
    {
        assert count <= buffers.size();
        if ( count == buffers.size() )
        {
            buffers.add( newBuffer() );
        }
        return clearBuffer( buffers.get( count++ ) );
    }

    private ByteBuffer clearBuffer( ByteBuffer buffer )
    {
        return buffer.clear().limit( bufferCapacity );
    }

    /**
     * @return {@code true} if there are more buffers before this one, otherwise {@code false} if there were no more buffers to go to.
     */
    boolean prev()
    {
        assert readIndex < count && readIndex >= 0;
        if ( readIndex > 0 )
        {
            readIndex--;
            return true;
        }
        return false;
    }

    /**
     * @return {@code true} if there are more buffers after this one, otherwise {@code false} if there were no more buffers to go to.
     */
    boolean next()
    {
        assert readIndex < count && readIndex >= 0;
        if ( readIndex + 1 < count )
        {
            readIndex++;
            return true;
        }
        return false;
    }

    ByteBuffer get()
    {
        // We iterate over the buffers backwards because the last one will likely be the smallest and so has a higher
        // chance to be packed together with other small parts in the same record.
        assert readIndex <= count;
        return buffers.get( readIndex );
    }

    int currentSize()
    {
        return this.count > 0 && readIndex < count && readIndex >= 0 ? buffers.get( readIndex ).limit() : 0;
    }

    int totalSize()
    {
        int size = 0;
        for ( int i = 0; i < count; i++ )
        {
            size += buffers.get( i ).limit();
        }
        return size;
    }

    private ByteBuffer newBuffer()
    {
        return ByteBuffer.wrap( new byte[bufferCapacity + LEEWAY_SIZE] ).limit( bufferCapacity );
    }

    ByteBuffer temp()
    {
        if ( tempBuffer == null )
        {
            tempBuffer = newBuffer();
        }
        return clearBuffer( tempBuffer );
    }

    IntermediateBuffer clear()
    {
        count = 0;
        readIndex = 0;
        return this;
    }

    void flip()
    {
        for ( int i = 0; i < count; i++ )
        {
            buffers.get( i ).flip();
        }
    }

    int limit()
    {
        int limit = 0;
        for ( int i = 0; i < count; i++ )
        {
            limit += buffers.get( i ).limit();
        }
        return limit;
    }

    int capacity()
    {
        return bufferCapacity;
    }

    boolean isSplit()
    {
        return count > 1;
    }
}
