/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.monitor;

import org.neo4j.logging.Log;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.helpers.Format.duration;

public class PageCacheWarmerLoggingMonitor extends PageCacheWarmerMonitorAdapter
{
    private final Log log;
    private long warmupStartMillis;

    public PageCacheWarmerLoggingMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void warmupStarted()
    {
        warmupStartMillis = currentTimeMillis();
        log.info( "Page cache warmup started." );
    }

    @Override
    public void warmupCompleted( long pagesLoaded )
    {
        log.info( "Page cache warmup completed. %d pages loaded. Duration: %s.", pagesLoaded, getDuration( warmupStartMillis ) );
    }

    private static String getDuration( long startTimeMillis )
    {
        return duration( currentTimeMillis() - startTimeMillis );
    }
}
