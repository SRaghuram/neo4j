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
<<<<<<< HEAD

import org.neo4j.io.pagecache.PageCursor;

interface SimpleBigValueStore extends SingleFileStore
{
    long allocateSpace( int length );

    void write( PageCursor cursor, ByteBuffer data, long position ) throws IOException;

    boolean read( PageCursor cursor, ByteBuffer data, long position ) throws IOException;

    int length( PageCursor cursor, long position ) throws IOException;

    long position();
=======
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.util.IdUpdateListener;

interface SimpleBigValueStore extends SingleFileStore
{
    List<Record> allocate( ByteBuffer data, PageCursorTracer cursorTracer );

    void write( PageCursor cursor, Iterable<Record> records, IdUpdateListener idUpdateListener, PageCursorTracer cursorTracer ) throws IOException;

    default ByteBuffer read( PageCursor cursor, long id ) throws IOException
    {
        return read( cursor, length -> ByteBuffer.wrap( new byte[length] ), id );
    }

    ByteBuffer read( PageCursor cursor, IntFunction<ByteBuffer> bufferCreator, long id ) throws IOException;

    void visitRecordChainIds( PageCursor cursor, long id, LongConsumer chainVisitor );

    long getHighId();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
}
