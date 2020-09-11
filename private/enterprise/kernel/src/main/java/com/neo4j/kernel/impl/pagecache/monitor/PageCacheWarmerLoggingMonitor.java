/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.monitor;

import org.neo4j.kernel.database.NamedDatabaseId;
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
    public void warmupStarted( NamedDatabaseId namedDatabaseId )
    {
        warmupStart = Stopwatch.start();
        log.info( "Page cache warmup started." );
    }

    @Override
    public void warmupCompleted( NamedDatabaseId namedDatabaseId, long pagesLoaded )
    {
        long millis = warmupStart.elapsed( MILLISECONDS );
        double pagesPerMilli = (double) pagesLoaded / millis;
        log.info( "Page cache warmup completed. %d pages loaded. Duration: %s. %.2f pages/ms", pagesLoaded, duration( millis ), pagesPerMilli );
    }
}
