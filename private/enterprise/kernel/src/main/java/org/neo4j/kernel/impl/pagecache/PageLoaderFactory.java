/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.pagecache;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

class PageLoaderFactory
{
    private final ExecutorService executor;
    private final PageCache pageCache;

    PageLoaderFactory( ExecutorService executor, PageCache pageCache )
    {
        this.executor = executor;
        this.pageCache = pageCache;
    }

    PageLoader getLoader( PagedFile file ) throws IOException
    {
        if ( FileUtils.highIODevice( file.file().toPath(), false ) )
        {
            return new ParallelPageLoader( file, executor, pageCache );
        }
        return new SingleCursorPageLoader( file );
    }
}
