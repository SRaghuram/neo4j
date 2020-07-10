/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import com.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitor;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.common.Subject.SYSTEM;

class WarmupAvailabilityListener implements AvailabilityListener
{
    private final JobScheduler scheduler;
    private final PageCacheWarmer pageCacheWarmer;
    private final Config config;
    private final Log log;
    private final PageCacheWarmerMonitor monitor;
    private final NamedDatabaseId namedDatabaseId;

    // We use the monitor lock to guard the job handle. However, it could happen that a job has already started, ends
    // up waiting for the lock while it's being held by another thread calling `unavailable()`. In that case, we need
    // to make sure that the signal to stop is not lost. Cancelling a job handle only works on jobs that haven't
    // started yet, since we don't propagate an interrupt. This is why we check the `available` field in the
    // `scheduleProfile` method.
    private volatile boolean available;
    private JobHandle jobHandle; // Guarded by `this`.

    WarmupAvailabilityListener( JobScheduler scheduler, PageCacheWarmer pageCacheWarmer,
                                Config config, Log log, PageCacheWarmerMonitor monitor, NamedDatabaseId namedDatabaseId )
    {
        this.scheduler = scheduler;
        this.pageCacheWarmer = pageCacheWarmer;
        this.config = config;
        this.log = log;
        this.monitor = monitor;
        this.namedDatabaseId = namedDatabaseId;
    }

    @Override
    public synchronized void available()
    {
        available = true;
        var monitoringParams = new JobMonitoringParams( SYSTEM, null, "Page cache warm up" );
        jobHandle = scheduler.schedule( Group.FILE_IO_HELPER, monitoringParams, this::startWarmup );
    }

    private void startWarmup()
    {
        if ( !available )
        {
            return;
        }
        try
        {
            monitor.warmupStarted( namedDatabaseId );
            pageCacheWarmer.reheat().ifPresent( loadedPages -> monitor.warmupCompleted( namedDatabaseId, loadedPages ) );
        }
        catch ( Exception e )
        {
            log.debug( "Active page cache warmup failed, " +
                       "so it may take longer for the cache to be populated with hot data.", e );
        }

        scheduleProfile();
    }

    private synchronized void scheduleProfile()
    {
        if ( !available )
        {
            return;
        }
        var monitoringParams = new JobMonitoringParams( SYSTEM, null, "Profiling of page cache" );
        long frequencyMillis = config.get( GraphDatabaseSettings.pagecache_warmup_profiling_interval ).toMillis();
        jobHandle = scheduler.scheduleRecurring( Group.FILE_IO_HELPER, monitoringParams, this::doProfile, frequencyMillis, TimeUnit.MILLISECONDS );
    }

    private void doProfile()
    {
        try
        {
            pageCacheWarmer.profile().ifPresent( pages -> monitor.profileCompleted( namedDatabaseId, pages ) );
        }
        catch ( Exception e )
        {
            log.debug( "Page cache profiling failed, so no new profile of what data is hot or not was produced. " +
                       "This may reduce the effectiveness of a future page cache warmup process.", e );
        }
    }

    @Override
    public synchronized void unavailable()
    {
        available = false;
        if ( jobHandle != null )
        {
            jobHandle.cancel();
            jobHandle = null;
        }
    }
}
