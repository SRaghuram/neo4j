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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.util.concurrent.WorkSync;

public class LabelIndexUpdatesWorkSync
{
    private WorkSync<EntityTokenUpdateListener,TokenUpdateWork> labelScanStoreSync;

    public LabelIndexUpdatesWorkSync( EntityTokenUpdateListener listener )
    {
        labelScanStoreSync = new WorkSync<>( listener );
    }

    public Batch newBatch()
    {
        return new Batch();
    }

    public class Batch
    {
        private List<EntityTokenUpdate> labelUpdates;

        public void add( EntityTokenUpdate update )
        {
            if ( labelUpdates == null )
            {
                labelUpdates = new ArrayList<>();
            }
            labelUpdates.add( update );
        }

        public AsyncApply applyAsync( PageCursorTracer cursorTracer )
        {
            return labelUpdates == null ? AsyncApply.EMPTY : labelScanStoreSync.applyAsync( new TokenUpdateWork( labelUpdates, cursorTracer ) );
        }

        public void apply( PageCursorTracer cursorTracer ) throws ExecutionException
        {
            if ( labelUpdates != null )
            {
                labelScanStoreSync.apply( new TokenUpdateWork( labelUpdates, cursorTracer ) );
            }
        }
    }
}
