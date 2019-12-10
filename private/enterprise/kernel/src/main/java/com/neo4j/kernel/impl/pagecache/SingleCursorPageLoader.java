/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

class SingleCursorPageLoader implements PageLoader
{
    private final PageCursor cursor;
    private final PageCursorTracer tracer;

    SingleCursorPageLoader( PagedFile file ) throws IOException
    {
        tracer = TRACER_SUPPLIER.get();
        cursor = file.io( 0, PF_SHARED_READ_LOCK, tracer );
    }

    @Override
    public void load( long pageId ) throws IOException
    {
        cursor.next( pageId );
    }

    @Override
    public void close()
    {
        closeAllUnchecked( cursor, cursor );
    }
}
