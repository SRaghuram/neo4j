/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import com.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerLoggingMonitor;
import com.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitor;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

@ServiceProvider
public class PageCacheWarmerExtensionFactory
        extends ExtensionFactory<PageCacheWarmerExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        JobScheduler jobScheduler();

        DatabaseAvailabilityGuard availabilityGuard();

        PageCache pageCache();

        FileSystemAbstraction fileSystemAbstraction();

        Database getDatabase();

        LogService logService();

        Monitors monitors();

        Config config();

        Tracers tracers();
    }

    public PageCacheWarmerExtensionFactory()
    {
        super( ExtensionType.DATABASE, "pagecachewarmer" );
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, Dependencies deps )
    {
        JobScheduler scheduler = deps.jobScheduler();
        DatabaseAvailabilityGuard databaseAvailabilityGuard = deps.availabilityGuard();
        PageCache pageCache = deps.pageCache();
        FileSystemAbstraction fs = deps.fileSystemAbstraction();
        LogService logService = deps.logService();
        Database database = deps.getDatabase();
        Log log = logService.getInternalLog( PageCacheWarmer.class );
        Monitors monitors = deps.monitors();
        PageCacheWarmerMonitor monitor = monitors.newMonitor( PageCacheWarmerMonitor.class );
        monitors.addMonitorListener( new PageCacheWarmerLoggingMonitor( log ) );
        Config config = deps.config();
        var tracers = deps.tracers();
        return new PageCacheWarmerExtension(
                scheduler, databaseAvailabilityGuard, pageCache, fs, database, log, monitor, config, tracers );
    }
}
