/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.List;

import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;

public class WriteUpdatesStep extends ProcessorStep<GenerateIndexUpdatesStep.GeneratedIndexUpdates>
{
    private final Visitor<List<EntityUpdates>,? extends Throwable> propertyUpdatesVisitor;
    private final Visitor<List<EntityTokenUpdate>,? extends Throwable> tokenUpdatesVisitor;

    public WriteUpdatesStep( StageControl control, Configuration config, Visitor<List<EntityUpdates>,? extends Exception> propertyUpdatesVisitor,
            Visitor<List<EntityTokenUpdate>,? extends Exception> tokenUpdatesVisitor, PageCacheTracer cacheTracer )
    {
        super( control, "write updates", config, 1, cacheTracer );
        this.propertyUpdatesVisitor = propertyUpdatesVisitor;
        this.tokenUpdatesVisitor = tokenUpdatesVisitor;
    }

    @Override
    protected void process( GenerateIndexUpdatesStep.GeneratedIndexUpdates updates, BatchSender sender, PageCursorTracer cursorTracer ) throws Exception
    {
        updates.accept( propertyUpdatesVisitor, tokenUpdatesVisitor );
    }
}
