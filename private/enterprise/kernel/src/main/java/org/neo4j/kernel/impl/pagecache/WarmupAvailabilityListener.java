/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.pagecache;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitor;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

class WarmupAvailabilityListener implements AvailabilityListener
{
    private final JobScheduler scheduler;
    private final PageCacheWarmer pageCacheWarmer;
    private final Config config;
    private final Log log;
    private final PageCacheWarmerMonitor monitor;

    // We use the monitor lock to guard the job handle. However, it could happen that a job has already started, ends
    // up waiting for the lock while it's being held by another thread calling `unavailable()`. In that case, we need
    // to make sure that the signal to stop is not lost. Cancelling a job handle only works on jobs that haven't
    // started yet, since we don't propagate an interrupt. This is why we check the `available` field in the
    // `scheduleProfile` method.
    private volatile boolean available;
    private JobHandle jobHandle; // Guarded by `this`.

    WarmupAvailabilityListener( JobScheduler scheduler, PageCacheWarmer pageCacheWarmer,
                                Config config, Log log, PageCacheWarmerMonitor monitor )
    {
        this.scheduler = scheduler;
        this.pageCacheWarmer = pageCacheWarmer;
        this.config = config;
        this.log = log;
        this.monitor = monitor;
    }

    @Override
    public synchronized void available()
    {
        available = true;
        jobHandle = scheduler.schedule( Group.FILE_IO_HELPER, this::startWarmup );
    }

    private void startWarmup()
    {
        if ( !available )
        {
            return;
        }
        try
        {
            monitor.warmupStarted();
            pageCacheWarmer.reheat().ifPresent( monitor::warmupCompleted );
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
        long frequencyMillis = config.get( GraphDatabaseSettings.pagecache_warmup_profiling_interval ).toMillis();
        jobHandle = scheduler.scheduleRecurring( Group.FILE_IO_HELPER, this::doProfile, frequencyMillis, TimeUnit.MILLISECONDS );
    }

    private void doProfile()
    {
        try
        {
            pageCacheWarmer.profile().ifPresent( monitor::profileCompleted );
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
            jobHandle.cancel( false );
            jobHandle = null;
        }
    }
}
