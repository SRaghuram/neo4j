/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.Optional;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.scheduler.JobHandle;

public class CoreDatabaseLife extends SafeLifecycle
{
    private final RaftMachine raftMachine;
    private final Database kernelDatabase;
    private final RaftBinder raftBinder;

    private final CommandApplicationProcess applicationProcess;
    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final RecoveryFacade recoveryFacade;

    public CoreDatabaseLife( RaftMachine raftMachine, Database kernelDatabase, RaftBinder raftBinder,
            CommandApplicationProcess commandApplicationProcess, LifecycleMessageHandler<?> raftMessageHandler,
            CoreSnapshotService snapshotService, CoreDownloaderService downloadService, RecoveryFacade recoveryFacade )
    {
        this.raftMachine = raftMachine;
        this.kernelDatabase = kernelDatabase;
        this.raftBinder = raftBinder;
        this.applicationProcess = commandApplicationProcess;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
        this.downloadService = downloadService;
        this.recoveryFacade = recoveryFacade;
    }

    @Override
    public void init0() throws Exception
    {
        kernelDatabase.init();
    }

    @Override
    public void start0() throws Exception
    {
        ensureRecovered();

        BoundState boundState = raftBinder.bindToRaft();
        raftMessageHandler.start( boundState.raftId() );

        boolean startedByDownloader = false;
        if ( boundState.snapshot().isPresent() )
        {
            // this means that we bootstrapped the cluster
            snapshotService.installSnapshot( boundState.snapshot().get() );
        }
        else
        {
            snapshotService.awaitState();
            Optional<JobHandle> downloadJob = downloadService.downloadJob();
            if ( downloadJob.isPresent() )
            {
                downloadJob.get().waitTermination();
                startedByDownloader = true;
            }
        }

        if ( !startedByDownloader )
        {
            kernelDatabase.start();
        }
        applicationProcess.start();
        raftMachine.postRecoveryActions();
    }

    private void ensureRecovered() throws Exception
    {
        recoveryFacade.recovery( kernelDatabase.getDatabaseLayout() );
    }

    @Override
    public void stop0() throws Exception
    {
        downloadService.stop();
        raftMachine.stopTimers();
        raftMessageHandler.stop();
        applicationProcess.stop();
        kernelDatabase.stop();
    }

    @Override
    public void shutdown0() throws Exception
    {
        kernelDatabase.shutdown();
    }
}
