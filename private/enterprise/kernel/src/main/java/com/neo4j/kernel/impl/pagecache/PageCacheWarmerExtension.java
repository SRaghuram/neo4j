/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import com.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitor;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

class PageCacheWarmerExtension extends LifecycleAdapter
{
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final Database database;
    private final Config config;
    private final PageCacheWarmer pageCacheWarmer;
    private final WarmupAvailabilityListener availabilityListener;
    private volatile boolean started;

    PageCacheWarmerExtension( JobScheduler scheduler, DatabaseAvailabilityGuard databaseAvailabilityGuard, PageCache pageCache, FileSystemAbstraction fs,
            Database database, Log log, PageCacheWarmerMonitor monitor, Config config, Tracers tracers )
    {
        this.databaseAvailabilityGuard = databaseAvailabilityGuard;
        this.database = database;
        this.config = config;
        this.pageCacheWarmer = new PageCacheWarmer( fs, pageCache, scheduler, database.getDatabaseLayout().databaseDirectory(),
                database.getNamedDatabaseId().name(), config, log, tracers );
        this.availabilityListener = new WarmupAvailabilityListener( scheduler, pageCacheWarmer, config, log, monitor, database.getNamedDatabaseId() );
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
    public void stop() throws Exception
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
        return database.getDependencyResolver().resolveDependency( DatabaseFileListing.class );
    }
}
