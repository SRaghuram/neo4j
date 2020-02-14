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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.internal.freki.InMemoryTestStore.NO_PAGE_CURSOR;

public class InMemoryBigValueTestStore extends LifecycleAdapter implements SimpleBigValueStore
{
    private final AtomicLong position = new AtomicLong();
    private final MutableLongObjectMap<byte[]> data = LongObjectMaps.mutable.empty();

    @Override
    public PageCursor openWriteCursor()
    {
        return NO_PAGE_CURSOR;
    }

    @Override
    public long allocateSpace( int length )
    {
        return this.position.getAndAdd( length );
    }

    @Override
    public void write( PageCursor cursor, ByteBuffer data, long position )
    {
        int length = data.remaining();
        byte[] dataCopy = new byte[length];
        System.arraycopy( data.array(), data.position(), dataCopy, 0, length );
        this.data.put( position, dataCopy );
    }

    @Override
    public PageCursor openReadCursor()
    {
        return NO_PAGE_CURSOR;
    }

    @Override
    public boolean read( PageCursor cursor, ByteBuffer data, long position )
    {
        byte[] bytes = this.data.get( position );
        if ( bytes == null )
        {
            return false;
        }
        System.arraycopy( bytes, 0, data.array(), data.position(), bytes.length );
        data.position( data.position() + bytes.length );
        return true;
    }

    @Override
    public void flush( PageCursorTracer cursorTracer )
    {
    }
}
