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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * Provide tree node (page) ids which can be used for storing tree node data.
 * Bytes on returned page ids must be empty (all zeros).
 */
interface IdProvider
{
    void visitFreelist( IdProviderVisitor visitor, PageCursorTracer cursorTracer ) throws IOException;

    long lastId();

    Writer writer( PageCursorTracer cursorTracer ) throws IOException;

    interface Writer extends AutoCloseable
    {
        /**
         * Acquires a page id, guaranteed to currently not be used. The bytes on the page at this id
         * are all guaranteed to be zero at the point of returning from this method.
         *
         * @param stableGeneration current stable generation.
         * @param unstableGeneration current unstable generation.
         * @param targetCursor cursor to use when zapping the page and the cursor which will be left at the acquired page which method returns.
         * @return page id guaranteed to current not be used and whose bytes are all zeros.
         * @throws IOException on {@link PageCursor} error.
         */
        long acquireNewId( long stableGeneration, long unstableGeneration, PageCursor targetCursor ) throws IOException;

        /**
         * Releases a page id which has previously been used, but isn't anymore, effectively allowing
         * it to be reused and returned from {@link #acquireNewId(long, long, PageCursor)}.
         *
         * @param stableGeneration current stable generation.
         * @param unstableGeneration current unstable generation.
         * @param id page id to release.
         * @throws IOException on {@link PageCursor} error.
         */
        void releaseId( long stableGeneration, long unstableGeneration, long id ) throws IOException;

        @Override
        void close();
    }

    interface IdProviderVisitor
    {
        void beginFreelistPage( long pageId );

        void endFreelistPage( long pageId );

        void freelistEntry( long pageId, long generation, int pos );

        class Adaptor implements IdProviderVisitor
        {
            @Override
            public void beginFreelistPage( long pageId )
            {
            }

            @Override
            public void endFreelistPage( long pageId )
            {
            }

            @Override
            public void freelistEntry( long pageId, long generation, int pos )
            {
            }
        }
    }
}
