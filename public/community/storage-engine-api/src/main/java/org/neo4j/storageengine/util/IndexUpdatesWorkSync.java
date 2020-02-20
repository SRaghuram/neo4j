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
package org.neo4j.storageengine.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.helpers.collection.NestingIterator;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

public class IndexUpdatesWorkSync
{
    private final WorkSync<IndexUpdateListener,IndexUpdatesWork> workSync;

    public IndexUpdatesWorkSync( IndexUpdateListener listener )
    {
        this.workSync = new WorkSync<>( listener );
    }

    public Batch newBatch()
    {
        return new Batch();
    }

    public class Batch
    {
        private final List<Iterable<IndexEntryUpdate<IndexDescriptor>>> updates = new ArrayList<>();

        public void add( Iterable<IndexEntryUpdate<IndexDescriptor>> indexUpdates )
        {
            updates.add( indexUpdates );
        }

        public void apply( PageCursorTracer cursorTracer ) throws ExecutionException
        {
            workSync.apply( new IndexUpdatesWork( updates, cursorTracer ) );
        }
    }

    /**
     * Combines index updates from multiple transactions into one bigger job.
     */
    private static class IndexUpdatesWork implements Work<IndexUpdateListener,IndexUpdatesWork>
    {
        private final List<Iterable<IndexEntryUpdate<IndexDescriptor>>> updates;
        private final PageCursorTracer cursorTracer;

        IndexUpdatesWork( List<Iterable<IndexEntryUpdate<IndexDescriptor>>> updates, PageCursorTracer cursorTracer )
        {
            this.cursorTracer = cursorTracer;
            this.updates = updates;
        }

        @Override
        public IndexUpdatesWork combine( IndexUpdatesWork work )
        {
            updates.addAll( work.updates );
            return this;
        }

        @Override
        public void apply( IndexUpdateListener material )
        {
            try
            {
                material.applyUpdates( combinedUpdates(), cursorTracer );
            }
            catch ( IOException | KernelException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }

        private Iterable<IndexEntryUpdate<IndexDescriptor>> combinedUpdates()
        {
            return () -> new NestingIterator<>( updates.iterator() )
            {
                @Override
                protected Iterator<IndexEntryUpdate<IndexDescriptor>> createNestedIterator( Iterable<IndexEntryUpdate<IndexDescriptor>> item )
                {
                    return item.iterator();
                }
            };
        }
    }
}
