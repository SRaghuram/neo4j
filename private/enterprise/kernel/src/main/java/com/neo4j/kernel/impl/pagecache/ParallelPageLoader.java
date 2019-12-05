/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

class ParallelPageLoader implements PageLoader
{
    private final PagedFile file;
    private final Executor executor;
    private final AtomicLong received;
    private final AtomicLong processed;

    ParallelPageLoader( PagedFile file, Executor executor )
    {
        this.file = file;
        this.executor = executor;
        received = new AtomicLong();
        processed = new AtomicLong();
    }

    @Override
    public void load( long pageId )
    {
        received.getAndIncrement();
        executor.execute( () ->
        {
            try
            {
                try ( PageCursorTracer cursorTracer = TRACER_SUPPLIER.get();
                      PageCursor cursor = file.io( pageId, PF_SHARED_READ_LOCK, cursorTracer ) )
                {
                    cursor.next();
                }
                catch ( IOException ignore )
                {
                }
            }
            finally
            {
                processed.getAndIncrement();
            }
        } );
    }

    @Override
    public void close()
    {
        while ( processed.get() < received.get() )
        {
            Thread.yield();
        }
    }
}
