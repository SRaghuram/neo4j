/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.monitor;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.time.Stopwatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.internal.helpers.Format.duration;

public class PageCacheWarmerLoggingMonitor extends PageCacheWarmerMonitorAdapter
{
    private final Log log;
    private Stopwatch warmupStart;

    public PageCacheWarmerLoggingMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void warmupStarted( DatabaseId databaseId )
    {
        warmupStart = Stopwatch.start();
        log.info( "Page cache warmup started." );
    }

    @Override
    public void warmupCompleted( DatabaseId databaseId, long pagesLoaded )
    {
        log.info( "Page cache warmup completed. %d pages loaded. Duration: %s.", pagesLoaded, duration( warmupStart.elapsed( MILLISECONDS ) ) );
    }
}
