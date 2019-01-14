/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.scheduler.JobScheduler;

class BackupPageCacheContainer implements AutoCloseable
{
    private final PageCache pageCache;
    private final JobScheduler jobScheduler;

    public static BackupPageCacheContainer of( PageCache pageCache )
    {
        return of( pageCache, null );
    }

    public static BackupPageCacheContainer of( PageCache pageCache, JobScheduler jobScheduler )
    {
        return new BackupPageCacheContainer( pageCache, jobScheduler );
    }

    private BackupPageCacheContainer( PageCache pageCache, JobScheduler jobScheduler )
    {
        this.pageCache = pageCache;
        this.jobScheduler = jobScheduler;
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    @Override
    public void close() throws Exception
    {
        pageCache.close();
        if ( jobScheduler != null )
        {
            jobScheduler.close();
        }
    }
}
