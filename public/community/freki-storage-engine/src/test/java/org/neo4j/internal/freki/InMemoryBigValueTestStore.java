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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
<<<<<<< HEAD
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
=======
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
<<<<<<< HEAD
import org.neo4j.storageengine.api.StorageCommand;

import static org.neo4j.internal.freki.InMemoryTestStore.NO_PAGE_CURSOR;

class InMemoryBigValueTestStore extends LifecycleAdapter implements SimpleBigValueStore
{
    static Consumer<StorageCommand> applyToStoreImmediately( SimpleBigValueStore store )
    {
        return command ->
        {
            FrekiCommand.BigPropertyValue valueCommand = (FrekiCommand.BigPropertyValue) command;
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                store.write( cursor, ByteBuffer.wrap( valueCommand.bytes ), valueCommand.pointer );
=======
import org.neo4j.storageengine.util.IdUpdateListener;

import static java.util.Collections.singletonList;
import static org.neo4j.internal.freki.InMemoryTestStore.NO_PAGE_CURSOR;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;

class InMemoryBigValueTestStore extends LifecycleAdapter implements SimpleBigValueStore
{
    static Consumer<FrekiCommand.BigPropertyValue> applyToStoreImmediately( SimpleBigValueStore store )
    {
        return command ->
        {
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                store.write( cursor, command.records, IdUpdateListener.DIRECT, PageCursorTracer.NULL );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        };
    }

<<<<<<< HEAD
    private final AtomicLong position = new AtomicLong();
=======
    private final AtomicLong highId = new AtomicLong();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    private final MutableLongObjectMap<byte[]> data = LongObjectMaps.mutable.empty();

    @Override
    public PageCursor openWriteCursor( PageCursorTracer cursorTracer )
    {
        return NO_PAGE_CURSOR;
    }

    @Override
<<<<<<< HEAD
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
=======
    public List<Record> allocate( ByteBuffer data, PageCursorTracer cursorTracer )
    {
        return singletonList( new Record( (byte) FLAG_IN_USE, highId.getAndIncrement(), data ) );
    }

    @Override
    public void write( PageCursor cursor, Iterable<Record> records, IdUpdateListener idUpdateListener, PageCursorTracer cursorTracer )
    {
        Iterator<Record> iterator = records.iterator();
        assert iterator.hasNext();
        Record single = iterator.next();
        assert !iterator.hasNext();
        if ( single.hasFlag( FLAG_IN_USE ) )
        {
            ByteBuffer data = single.data();
            int length = data.remaining();
            byte[] dataCopy = new byte[length];
            System.arraycopy( data.array(), data.position(), dataCopy, 0, length );
            assert !this.data.containsKey( single.id );
            this.data.put( single.id, dataCopy );
        }
        else
        {
            assert this.data.containsKey( single.id );
            this.data.remove( single.id );
        }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public PageCursor openReadCursor( PageCursorTracer cursorTracer )
    {
        return NO_PAGE_CURSOR;
    }

    @Override
<<<<<<< HEAD
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
    public int length( PageCursor cursor, long position )
    {
        byte[] bytes = this.data.get( position );
        if ( bytes != null )
        {
            return bytes.length;
        }
        return -1;
    }

    @Override
    public void flush( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
    {
    }

    @Override
    public long position()
    {
        return position.get();
=======
    public ByteBuffer read( PageCursor cursor, IntFunction<ByteBuffer> bufferCreator, long id )
    {
        byte[] bytes = this.data.get( id );
        if ( bytes == null )
        {
            return null;
        }
        ByteBuffer data = bufferCreator.apply( bytes.length );
        System.arraycopy( bytes, 0, data.array(), data.position(), bytes.length );
        return data;
    }

    @Override
    public void flush( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
    {
    }

    @Override
    public boolean exists( PageCursor cursor, long id )
    {
        return data.containsKey( id );
    }

    @Override
    public void visitRecordChainIds( PageCursor cursor, long id, LongConsumer chainVisitor )
    {
        chainVisitor.accept( id );
    }

    @Override
    public long getHighId()
    {
        return highId.get();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }
}
