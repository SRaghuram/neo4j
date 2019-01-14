/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.pagecache.monitor;

public interface PageCacheWarmerMonitor
{
    void warmupStarted();

    void warmupCompleted( long pagesLoaded );

    void profileCompleted( long pagesInMemory );
}
