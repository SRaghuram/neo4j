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

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

/**
 * Convenience for updating one or more {@link IdGenerator} in a concurrent fashion. Supports applying in batches, e.g. multiple transactions
 * in one go, see {@link #newBatch(PageCacheTracer)}.
 */
public class IdGeneratorUpdatesWorkSync
{
    public static final String ID_GENERATOR_BATCH_APPLIER_TAG = "idGeneratorBatchApplier";

    private final Map<IdGenerator,WorkSync<IdGenerator,IdGeneratorUpdateWork>> workSyncMap = new HashMap<>();

    public void add( IdGenerator idGenerator )
    {
        this.workSyncMap.put( idGenerator, new WorkSync<>( idGenerator ) );
    }

    public Batch newBatch( PageCacheTracer pageCacheTracer )
    {
        return new Batch( pageCacheTracer );
    }

    public class Batch implements IdUpdateListener
    {
        private final Map<IdGenerator,ChangedIds> idUpdatesMap = new HashMap<>();
        private PageCacheTracer pageCacheTracer;

        protected Batch( PageCacheTracer pageCacheTracer )
        {
            this.pageCacheTracer = pageCacheTracer;
        }

        @Override
        public void markIdAsUsed( IdGenerator idGenerator, long id, PageCursorTracer cursorTracer )
        {
            idUpdatesMap.computeIfAbsent( idGenerator, t -> new ChangedIds() ).addUsedId( id );
        }

        @Override
        public void markIdAsUnused( IdGenerator idGenerator, long id, PageCursorTracer cursorTracer )
        {
            idUpdatesMap.computeIfAbsent( idGenerator, t -> new ChangedIds() ).addUnusedId( id );
        }

        public AsyncApply applyAsync( PageCacheTracer cacheTracer )
        {
            // Run through the id changes and apply them, or rather apply them asynchronously.
            // This allows multiple concurrent threads applying batches of transactions to help each other out so that
            // there's a higher chance that changes to different id types can be applied in parallel.
            applyInternal( cacheTracer );
            return this::awaitApply;
        }

        public void apply( PageCacheTracer cacheTracer ) throws ExecutionException
        {
            applyInternal( cacheTracer );
            awaitApply();
        }

        private void awaitApply() throws ExecutionException
        {
            // Wait for all id updates to complete
            for ( Map.Entry<IdGenerator,ChangedIds> idChanges : idUpdatesMap.entrySet() )
            {
                ChangedIds unit = idChanges.getValue();
                unit.awaitApply();
            }
        }

        private void applyInternal( PageCacheTracer cacheTracer )
        {
            for ( Map.Entry<IdGenerator,ChangedIds> idChanges : idUpdatesMap.entrySet() )
            {
                ChangedIds unit = idChanges.getValue();
                unit.applyAsync( workSyncMap.get( idChanges.getKey() ), cacheTracer );
            }
        }

        @Override
        public void close() throws Exception
        {
            apply( pageCacheTracer );
        }
    }

    private static class ChangedIds
    {
        private final MutableLongList ids = LongLists.mutable.empty();
        private final BitSet usedSet = new BitSet(); // set=used, cleared=unused
        private AsyncApply asyncApply;

        private void addUsedId( long id )
        {
            usedSet.set( ids.size() );
            ids.add( id );
        }

        void addUnusedId( long id )
        {
            ids.add( id );
        }

        void accept( IdGenerator.Marker visitor )
        {
            ids.forEachWithIndex( ( id, index ) ->
            {
                boolean used = usedSet.get( index );
                if ( used )
                {
                    visitor.markUsed( id );
                }
                else
                {
                    visitor.markDeleted( id );
                }
            } );
        }

        void applyAsync( WorkSync<IdGenerator,IdGeneratorUpdateWork> workSync, PageCacheTracer cacheTracer )
        {
            asyncApply = workSync.applyAsync( new IdGeneratorUpdateWork( this, cacheTracer ) );
        }

        void awaitApply() throws ExecutionException
        {
            asyncApply.await();
        }
    }

    private static class IdGeneratorUpdateWork implements Work<IdGenerator,IdGeneratorUpdateWork>
    {
        private final List<ChangedIds> changeList = new ArrayList<>();
        private final PageCacheTracer cacheTracer;

        IdGeneratorUpdateWork( ChangedIds changes, PageCacheTracer cacheTracer )
        {
            this.cacheTracer = cacheTracer;
            this.changeList.add( changes );
        }

        @Override
        public IdGeneratorUpdateWork combine( IdGeneratorUpdateWork work )
        {
            changeList.addAll( work.changeList );
            return this;
        }

        @Override
        public void apply( IdGenerator idGenerator )
        {
            try ( var cursorTracer = cacheTracer.createPageCursorTracer( ID_GENERATOR_BATCH_APPLIER_TAG );
                    IdGenerator.Marker marker = idGenerator.marker( cursorTracer ) )
            {
                for ( ChangedIds changes : this.changeList )
                {
                    changes.accept( marker );
                }
            }
        }
    }
}
