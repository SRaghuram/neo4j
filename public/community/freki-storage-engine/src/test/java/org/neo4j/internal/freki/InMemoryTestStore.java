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

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.util.IdUpdateListener;

class InMemoryTestStore extends LifecycleAdapter implements SimpleStore
{
    private final MutableLongObjectMap<Record> data = LongObjectMaps.mutable.empty();
    private final int sizeExp;
    private final AtomicLong nextId = new AtomicLong();

    InMemoryTestStore( int sizeExp )
    {
        this.sizeExp = sizeExp;
    }

    @Override
    public int recordSize()
    {
        return Record.recordSize( sizeExp );
    }

    @Override
    public int recordSizeExponential()
    {
        return sizeExp;
    }

    @Override
    public Record newRecord()
    {
        return new Record( sizeExp );
    }

    @Override
    public Record newRecord( long id )
    {
        return new Record( sizeExp, id );
    }

    @Override
    public PageCursor openWriteCursor()
    {
        return NO_PAGE_CURSOR;
    }

    @Override
    public void write( PageCursor cursor, Record record, IdUpdateListener idUpdateListener, PageCursorTracer cursorTracer )
    {
        Record copy = new Record( 1, 0 );
        copy.copyContentsFrom( record );
        data.put( record.id, copy );
    }

    @Override
    public PageCursor openReadCursor()
    {
        return NO_PAGE_CURSOR;
    }

    @Override
    public boolean read( PageCursor cursor, Record record, long id )
    {
        Record source = data.get( id );
        if ( source == null )
        {
            return false;
        }
        record.copyContentsFrom( source );
        return true;
    }

    @Override
    public void flush( PageCursorTracer cursorTracer )
    {
    }

    @Override
    public long nextId( PageCursorTracer cursorTracer )
    {
        return nextId.getAndIncrement();
    }

    @Override
    public long getHighId()
    {
        return nextId.get();
    }

    @Override
    public boolean exists( long id )
    {
        return data.contains( id );
    }

    @Override
    public void close() throws Exception
    {
        shutdown();
    }

    // Basically this isn't used, it's just something to call close()
    static PageCursor NO_PAGE_CURSOR = new ByteArrayPageCursor( new byte[0] );
}
