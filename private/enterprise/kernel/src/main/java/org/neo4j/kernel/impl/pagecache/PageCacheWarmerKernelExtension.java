/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.pagecache;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitor;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

class PageCacheWarmerKernelExtension extends LifecycleAdapter
{
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final Database dataSource;
    private final Config config;
    private final PageCacheWarmer pageCacheWarmer;
    private final WarmupAvailabilityListener availabilityListener;
    private volatile boolean started;

    PageCacheWarmerKernelExtension(
            JobScheduler scheduler, DatabaseAvailabilityGuard databaseAvailabilityGuard, PageCache pageCache, FileSystemAbstraction fs,
            Database dataSource, Log log, PageCacheWarmerMonitor monitor, Config config )
    {
        this.databaseAvailabilityGuard = databaseAvailabilityGuard;
        this.dataSource = dataSource;
        this.config = config;
        pageCacheWarmer = new PageCacheWarmer( fs, pageCache, scheduler, dataSource.getDatabaseLayout().databaseDirectory() );
        availabilityListener = new WarmupAvailabilityListener( scheduler, pageCacheWarmer, config, log, monitor );
    }

    @Override
    public void start()
    {
        if ( config.get( GraphDatabaseSettings.pagecache_warmup_enabled ) )
        {
            pageCacheWarmer.start();
            databaseAvailabilityGuard.addListener( availabilityListener );
            getNeoStoreFileListing().registerStoreFileProvider( pageCacheWarmer );
            started = true;
        }
    }

    @Override
    public void stop() throws Throwable
    {
        if ( started )
        {
            databaseAvailabilityGuard.removeListener( availabilityListener );
            availabilityListener.unavailable(); // Make sure scheduled jobs get cancelled.
            pageCacheWarmer.stop();
            started = false;
        }
    }

    private DatabaseFileListing getNeoStoreFileListing()
    {
        return dataSource.getDependencyResolver().resolveDependency( DatabaseFileListing.class );
    }
}
