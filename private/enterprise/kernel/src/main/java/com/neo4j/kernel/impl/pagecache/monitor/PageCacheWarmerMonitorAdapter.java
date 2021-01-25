/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.monitor;

import org.neo4j.kernel.database.NamedDatabaseId;

public class PageCacheWarmerMonitorAdapter implements PageCacheWarmerMonitor
{
    @Override
    public void warmupStarted( NamedDatabaseId namedDatabaseId )
    {
        //nothing
    }

    @Override
    public void warmupCompleted( NamedDatabaseId namedDatabaseId, long pagesLoaded )
    {
        //nothing
    }

    @Override
    public void profileCompleted( NamedDatabaseId namedDatabaseId, long pagesInMemory )
    {
        //nothing
    }
}
