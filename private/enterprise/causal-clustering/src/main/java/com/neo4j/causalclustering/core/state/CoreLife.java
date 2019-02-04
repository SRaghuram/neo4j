/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.CoreDatabaseContext;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.ClusterBinder;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.Map;
import java.util.Optional;

import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobHandle;

public class CoreLife extends SafeLifecycle
{
    private final RaftMachine raftMachine;
    private final ClusteredDatabaseManager<CoreDatabaseContext> databaseManager;
    private final ClusterBinder clusterBinder;

    private final CommandApplicationProcess applicationProcess;
    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final RecoveryFacade recoveryFacade;

    public CoreLife( RaftMachine raftMachine, ClusteredDatabaseManager<CoreDatabaseContext> databaseManager, ClusterBinder clusterBinder,
            CommandApplicationProcess commandApplicationProcess, LifecycleMessageHandler<?> raftMessageHandler,
            CoreSnapshotService snapshotService, CoreDownloaderService downloadService, LogProvider logProvider,
            RecoveryFacade recoveryFacade )
    {
        this.raftMachine = raftMachine;
        this.databaseManager = databaseManager;
        this.clusterBinder = clusterBinder;
        this.applicationProcess = commandApplicationProcess;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
        this.downloadService = downloadService;
        this.recoveryFacade = recoveryFacade;
    }

    @Override
    public void init0() throws Exception
    {
        databaseManager.init();
    }

    @Override
    public void start0() throws Exception
    {
        ensureRecovered();

        BoundState boundState = clusterBinder.bindToCluster();
        raftMessageHandler.start( boundState.clusterId() );

        boolean startedByDownloader = false;
        if ( !boundState.snapshots().isEmpty() )
        {
            // this means that we bootstrapped the cluster
            for ( Map.Entry<String,CoreSnapshot> entry : boundState.snapshots().entrySet() )
            {
                snapshotService.installSnapshot( entry.getKey(), entry.getValue() );
            }
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
            databaseManager.start();
        }
        applicationProcess.start();
        raftMachine.postRecoveryActions();
    }

    private void ensureRecovered() throws Exception
    {
        for ( ClusteredDatabaseContext db : databaseManager.registeredDatabases().values() )
        {
            recoveryFacade.recovery( db.databaseLayout() );
        }
    }

    @Override
    public void stop0() throws Exception
    {
        raftMachine.stopTimers();
        raftMessageHandler.stop();
        applicationProcess.stop();
        databaseManager.stop();
    }

    @Override
    public void shutdown0() throws Exception
    {
        databaseManager.shutdown();
    }
}
