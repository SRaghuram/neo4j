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
    private int index;
    private int readIndex;
    private ByteBuffer tempBuffer;

    IntermediateBuffer( int bufferCapacity )
    {
        this.bufferCapacity = bufferCapacity;
    }

    ByteBuffer add()
    {
        assert index <= buffers.size();
        if ( index == buffers.size() )
        {
            buffers.add( newBuffer() );
        }
        return buffers.get( index++ ).clear().limit( bufferCapacity );
    }

    ByteBuffer get()
    {
        // We iterate over the buffers backwards because the last one will likely be the smallest and so has a higher
        // chance to be packed together with other small parts in the same record.
        assert readIndex < index;
        return buffers.get( index - readIndex++ - 1 );
    }

    int currentSize()
    {
        return index > 0 ? buffers.get( index - readIndex - 1 ).limit() : 0;
    }

    int totalSize()
    {
        int size = 0;
        for ( int i = 0; i < index; i++ )
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
        return tempBuffer.clear();
    }

    IntermediateBuffer clear()
    {
        index = 0;
        readIndex = 0;
        return this;
    }

    void flip()
    {
        for ( int i = 0; i < index; i++ )
        {
            buffers.get( i ).flip();
        }
    }

    int limit()
    {
        int limit = 0;
        for ( int i = 0; i < index; i++ )
        {
            limit += buffers.get( i ).limit();
        }
        return limit;
    }

    int capacity()
    {
        return bufferCapacity;
    }
}
