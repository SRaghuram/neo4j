/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class BackupPageCacheContainerTest
{
    private PageCache pageCache;
    private JobScheduler jobScheduler;

    @BeforeEach
    void setUp()
    {
        pageCache = mock( PageCache.class );
        jobScheduler = mock( JobScheduler.class );
    }

    @Test
    void createAndCloseContainerWithPageCacheOnly() throws Exception
    {
        BackupPageCacheContainer pageCacheContainer = BackupPageCacheContainer.of( pageCache );
        pageCacheContainer.close();

        verify( pageCache ).close();
        verify( jobScheduler, never() ).close();
    }

    @Test
    void createAndCloseContainerWithPageCacheAndScheduler() throws Exception
    {
        BackupPageCacheContainer pageCacheContainer = BackupPageCacheContainer.of( pageCache, jobScheduler );
        pageCacheContainer.close();

        verify( pageCache ).close();
        verify( jobScheduler ).close();
    }
}
