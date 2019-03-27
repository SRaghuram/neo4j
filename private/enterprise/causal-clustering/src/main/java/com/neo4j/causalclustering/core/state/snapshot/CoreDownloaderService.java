/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.Suspendable;
import com.neo4j.causalclustering.helper.TimeoutStrategy;

import java.util.Optional;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

public class CoreDownloaderService extends LifecycleAdapter
{
    private final JobScheduler jobScheduler;
    private final CoreDownloader downloader;
    private final Suspendable suspendOnStoreCopy;
    private final ClusteredDatabaseManager<?> databaseManager;
    private final CommandApplicationProcess applicationProcess;
    private final Log log;
    private final TimeoutStrategy backoffStrategy;
    private final Monitors monitors;
    private final CoreSnapshotService snapshotService;

    private PersistentSnapshotDownloader currentJob;
    private JobHandle jobHandle;
    private boolean stopped;
    private Panicker panicker;

    public CoreDownloaderService( JobScheduler jobScheduler, CoreDownloader downloader, CoreSnapshotService snapshotService, Suspendable suspendOnStoreCopy,
            ClusteredDatabaseManager<?> databaseManager, CommandApplicationProcess applicationProcess,
            LogProvider logProvider, TimeoutStrategy backoffStrategy,
            Panicker panicker, Monitors monitors )
    {
        this.jobScheduler = jobScheduler;
        this.downloader = downloader;
        this.snapshotService = snapshotService;
        this.suspendOnStoreCopy = suspendOnStoreCopy;
        this.databaseManager = databaseManager;
        this.applicationProcess = applicationProcess;
        this.log = logProvider.getLog( getClass() );
        this.backoffStrategy = backoffStrategy;
        this.panicker = panicker;
        this.monitors = monitors;
    }

    public synchronized Optional<JobHandle> scheduleDownload( CatchupAddressProvider addressProvider )
    {
        if ( stopped )
        {
            return Optional.empty();
        }

        if ( currentJob == null || currentJob.hasCompleted() )
        {
            currentJob = new PersistentSnapshotDownloader( addressProvider, applicationProcess, suspendOnStoreCopy, databaseManager, downloader,
                    snapshotService, log, backoffStrategy, panicker, monitors );
            jobHandle = jobScheduler.schedule( Group.DOWNLOAD_SNAPSHOT, currentJob );
            return Optional.of( jobHandle );
        }
        return Optional.of( jobHandle );
    }

    @Override
    public synchronized void stop() throws Exception
    {
        stopped = true;

        if ( currentJob != null )
        {
            currentJob.stop();
        }
    }

    public synchronized Optional<JobHandle> downloadJob()
    {
        return Optional.ofNullable( jobHandle );
    }
}
