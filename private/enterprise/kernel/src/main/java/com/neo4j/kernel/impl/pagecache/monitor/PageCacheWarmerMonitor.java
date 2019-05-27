/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.monitor;

import org.neo4j.kernel.database.DatabaseId;

public interface PageCacheWarmerMonitor
{
    void warmupStarted( DatabaseId databaseId );

    void warmupCompleted( DatabaseId databaseId, long pagesLoaded );

    void profileCompleted( DatabaseId databaseId, long pagesInMemory );
}
