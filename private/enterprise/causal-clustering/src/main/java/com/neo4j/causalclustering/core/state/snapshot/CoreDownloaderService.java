/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.ReplicatedDatabaseEventService;

import java.util.Optional;

import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.dbms.DatabaseStartAborter.PreventReason.STORE_COPY;

public class CoreDownloaderService extends LifecycleAdapter
{
    private final JobScheduler jobScheduler;
    private final CoreDownloader downloader;
    private final CommandApplicationProcess applicationProcess;
    private final Log log;
    private final TimeoutStrategy backoffStrategy;
    private final DatabasePanicker panicker;
    private final Monitors monitors;
    private final DatabaseStartAborter databaseStartAborter;
    private final StoreDownloadContext context;
    private final CoreSnapshotService snapshotService;
    private final ReplicatedDatabaseEventService databaseEventService;

    private PersistentSnapshotDownloader currentJob;
    private JobHandle jobHandle;
    private boolean stopped;

    public CoreDownloaderService( JobScheduler jobScheduler, CoreDownloader downloader, StoreDownloadContext context, CoreSnapshotService snapshotService,
            ReplicatedDatabaseEventService databaseEventService, CommandApplicationProcess applicationProcess, LogProvider logProvider,
            TimeoutStrategy backoffStrategy, DatabasePanicker panicker, Monitors monitors, DatabaseStartAborter databaseStartAborter )
    {
        this.jobScheduler = jobScheduler;
        this.downloader = downloader;
        this.context = context;
        this.snapshotService = snapshotService;
        this.databaseEventService = databaseEventService;
        this.applicationProcess = applicationProcess;
        this.log = logProvider.getLog( getClass() );
        this.backoffStrategy = backoffStrategy;
        this.panicker = panicker;
        this.monitors = monitors;
        this.databaseStartAborter = databaseStartAborter;
    }

    public synchronized Optional<JobHandle> scheduleDownload( CatchupAddressProvider addressProvider )
    {
        if ( stopped )
        {
            return Optional.empty();
        }

        if ( currentJob == null || currentJob.hasCompleted() )
        {
            currentJob = new PersistentSnapshotDownloader( addressProvider, applicationProcess, downloader, snapshotService, databaseEventService, context, log,
                    backoffStrategy, panicker, monitors );
            jobHandle = jobScheduler.schedule( Group.DOWNLOAD_SNAPSHOT, currentJob );
            return Optional.of( jobHandle );
        }
        return Optional.of( jobHandle );
    }

    @Override
    public synchronized void start() throws Exception
    {
        /* We prevent aborts while running so that the kernel cannot abort recovering
           the database. This can otherwise happen during the restart of the kernel database
           in the PersistentSnapshotDownloader and will cause subsequent issues. */
        databaseStartAborter.setAbortable( context.databaseId(), STORE_COPY, false );
    }

    @Override
    public synchronized void stop() throws Exception
    {
        stopped = true;

        if ( currentJob != null )
        {
            currentJob.stop();
        }

        databaseStartAborter.setAbortable( context.databaseId(), STORE_COPY, true );
    }

    public synchronized Optional<JobHandle> downloadJob()
    {
        return Optional.ofNullable( jobHandle );
    }
}
