/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.monitor;

import org.neo4j.kernel.database.NamedDatabaseId;

public interface PageCacheWarmerMonitor
{
    void warmupStarted( NamedDatabaseId namedDatabaseId );

    void warmupCompleted( NamedDatabaseId namedDatabaseId, long pagesLoaded );

    void profileCompleted( NamedDatabaseId namedDatabaseId, long pagesInMemory );
}
