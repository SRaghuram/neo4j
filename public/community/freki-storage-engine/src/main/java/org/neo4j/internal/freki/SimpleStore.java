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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.util.IdUpdateListener;

public interface SimpleStore extends SingleFileStore
{
    int recordSize();

    int recordSizeExponential();

    Record newRecord();

    Record newRecord( long id );

    void write( PageCursor cursor, Record record, IdUpdateListener idUpdateListener, PageCursorTracer cursorTracer ) throws IOException;

    boolean read( PageCursor cursor, Record record, long id );

    long nextId( PageCursorTracer cursorTracer );

    boolean exists( PageCursor cursor, long id ) throws IOException;

    long getHighId();
}