/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.ClusteredDatabaseLife;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import java.util.Optional;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.scheduler.JobHandle;


public class CoreDatabaseLife implements ClusteredDatabaseLife
{
    private final RaftMachine raftMachine;
    private final Database kernelDatabase;
    private final RaftBinder raftBinder;

    private final CommandApplicationProcess applicationProcess;
    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final RecoveryFacade recoveryFacade;
    private final CoreTopologyService topologyService;
    private final LifeSupport clusterComponentsLife;
    private final ClusterInternalDbmsOperator clusterInternalOperator;

    public CoreDatabaseLife( RaftMachine raftMachine, Database kernelDatabase, RaftBinder raftBinder, CommandApplicationProcess commandApplicationProcess,
            LifecycleMessageHandler<?> raftMessageHandler, CoreSnapshotService snapshotService, CoreDownloaderService downloadService,
            RecoveryFacade recoveryFacade, LifeSupport clusterComponentsLife, ClusterInternalDbmsOperator clusterInternalOperator,
            CoreTopologyService topologyService )
    {
        this.raftMachine = raftMachine;
        this.kernelDatabase = kernelDatabase;
        this.raftBinder = raftBinder;
        this.applicationProcess = commandApplicationProcess;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
        this.downloadService = downloadService;
        this.recoveryFacade = recoveryFacade;
        this.clusterComponentsLife = clusterComponentsLife;
        this.clusterInternalOperator = clusterInternalOperator;
        this.topologyService = topologyService;
    }

    @Override
    public void init() throws Exception
    {
        var signal = clusterInternalOperator.bootstrap( kernelDatabase.getDatabaseId() );
        clusterComponentsLife.init();
        kernelDatabase.init();
        bootstrap();
        signal.bootstrapped();
    }

    private void bootstrap() throws Exception
    {
        clusterComponentsLife.start();
        ensureRecovered();

        topologyService.onDatabaseStart( kernelDatabase.getDatabaseId() );
        BoundState boundState = raftBinder.bindToRaft();
        raftMessageHandler.start( boundState.raftId() );

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
            }
        }
    }

    @Override
    public void start() throws Exception
    {
        kernelDatabase.start();
        applicationProcess.start();
        raftMachine.postRecoveryActions();
    }

    private void ensureRecovered() throws Exception
    {
        recoveryFacade.recovery( kernelDatabase.getDatabaseLayout() );
    }

    @Override
    public void stop() throws Exception
    {
        topologyService.onDatabaseStop( kernelDatabase.getDatabaseId() );
        downloadService.stop();
        raftMachine.stopTimers();
        raftMessageHandler.stop();
        applicationProcess.stop();
        kernelDatabase.stop();
        clusterComponentsLife.stop();
    }

    @Override
    public void shutdown() throws Exception
    {
        kernelDatabase.shutdown();
        clusterComponentsLife.shutdown();
    }

    @Override
    public void add( Lifecycle lifecycledComponent )
    {
        clusterComponentsLife.add( lifecycledComponent );
    }
}
