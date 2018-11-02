/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.pagecache.monitor;

public class PageCacheWarmerMonitorAdapter implements PageCacheWarmerMonitor
{
    @Override
    public void warmupStarted()
    {
        //nothing
    }

    @Override
    public void warmupCompleted( long pagesLoaded )
    {
        //nothing
    }

    @Override
    public void profileCompleted( long pagesInMemory )
    {
        //nothing
    }
}
