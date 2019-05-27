/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.monitor;

import org.neo4j.kernel.database.DatabaseId;

public class PageCacheWarmerMonitorAdapter implements PageCacheWarmerMonitor
{
    @Override
    public void warmupStarted( DatabaseId databaseId )
    {
        //nothing
    }

    @Override
    public void warmupCompleted( DatabaseId databaseId, long pagesLoaded )
    {
        //nothing
    }

    @Override
    public void profileCompleted( DatabaseId databaseId, long pagesInMemory )
    {
        //nothing
    }
}
