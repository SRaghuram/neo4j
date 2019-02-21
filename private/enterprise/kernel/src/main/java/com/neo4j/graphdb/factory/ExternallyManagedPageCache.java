/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb.factory;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.Optional;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

/**
 * A PageCache implementation that delegates to another page cache, whose life cycle is managed elsewhere.
 *
 * This page cache implementation DOES NOT delegate close() method calls, so it can be used to safely share a page
 * cache with a component that might try to close the page cache it gets.
 */
public class ExternallyManagedPageCache implements PageCache
{
    private final PageCache delegate;

    private ExternallyManagedPageCache( PageCache delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void close()
    {
        // Don't close the delegate, because we are not in charge of its life cycle.
    }

    @Override
    public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
    {
        return delegate.map( file, pageSize, openOptions );
    }

    @Override
    public Optional<PagedFile> getExistingMapping( File file ) throws IOException
    {
        return delegate.getExistingMapping( file );
    }

    @Override
    public List<PagedFile> listExistingMappings() throws IOException
    {
        return delegate.listExistingMappings();
    }

    @Override
    public void flushAndForce() throws IOException
    {
        delegate.flushAndForce();
    }

    @Override
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        delegate.flushAndForce( limiter );
    }

    @Override
    public int pageSize()
    {
        return delegate.pageSize();
    }

    @Override
    public long maxCachedPages()
    {
        return delegate.maxCachedPages();
    }

    @Override
    public void reportEvents()
    {
        delegate.reportEvents();
    }
}
