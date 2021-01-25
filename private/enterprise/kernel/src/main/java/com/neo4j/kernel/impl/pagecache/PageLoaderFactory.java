/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

class PageLoaderFactory
{
    private final ExecutorService executor;

    PageLoaderFactory( ExecutorService executor )
    {
        this.executor = executor;
    }

    PageLoader getLoader( PagedFile file, PageCacheTracer tracer ) throws IOException
    {
        if ( FileUtils.highIODevice( file.path() ) )
        {
            return new ParallelPageLoader( file, executor, tracer );
        }
        return new SingleCursorPageLoader( file, tracer );
    }
}
