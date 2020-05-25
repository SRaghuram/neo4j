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

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

class SimpleIdProvider implements IdProvider, IdProvider.Writer
{
    private final Queue<Pair<Long,Long>> releasedIds = new LinkedList<>();
    private long lastId;

    SimpleIdProvider()
    {
        reset();
    }

    @Override
    public Writer writer( PageCursorTracer cursorTracer )
    {
        return this;
    }

    @Override
    public long acquireNewId( long stableGeneration, long unstableGeneration, PageCursor targetCursor ) throws IOException
    {
        if ( !releasedIds.isEmpty() )
        {
            Pair<Long,Long> free = releasedIds.peek();
            if ( free.getLeft() <= stableGeneration )
            {
                releasedIds.poll();
                long pageId = free.getRight();
                targetCursor.next( pageId );
                targetCursor.zapPage();
                return pageId;
            }
        }
        lastId++;
        targetCursor.next( lastId );
        return lastId;
    }

    @Override
    public void releaseId( long stableGeneration, long unstableGeneration, long id )
    {
        releasedIds.add( Pair.of( unstableGeneration, id ) );
    }

    @Override
    public void visitFreelist( IdProviderVisitor visitor, PageCursorTracer cursorTracer )
    {
        int pos = 0;
        visitor.beginFreelistPage( 0 );
        for ( Pair<Long,Long> releasedId : releasedIds )
        {
            visitor.freelistEntry( releasedId.getRight(), releasedId.getLeft(), pos++ );
        }
        visitor.endFreelistPage( 0 );
    }

    @Override
    public long lastId()
    {
        return lastId;
    }

    @Override
    public void close()
    {
    }

    void reset()
    {
        releasedIds.clear();
        lastId = IdSpace.MIN_TREE_NODE_ID - 1;
    }
}
