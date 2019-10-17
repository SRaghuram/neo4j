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
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import java.util.List;
import java.util.Optional;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.scheduler.JobHandle;

import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.markUnhealthy;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.raiseAvailabilityGuard;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.stopDatabase;

public class CoreDatabaseLife extends ClusteredDatabaseLife
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
    private final PanicService panicService;

    public CoreDatabaseLife( RaftMachine raftMachine, Database kernelDatabase, RaftBinder raftBinder, CommandApplicationProcess commandApplicationProcess,
            LifecycleMessageHandler<?> raftMessageHandler, CoreSnapshotService snapshotService, CoreDownloaderService downloadService,
            RecoveryFacade recoveryFacade, LifeSupport clusterComponentsLife, ClusterInternalDbmsOperator clusterInternalOperator,
            CoreTopologyService topologyService, PanicService panicService )
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
        this.panicService = panicService;
    }

    @Override
    protected void start0() throws Exception
    {
        addPanicEventHandlers();
        bootstrap();

        kernelDatabase.start();
        applicationProcess.start();
        raftMachine.postRecoveryActions();
    }

    private void bootstrap() throws Exception
    {
        var signal = clusterInternalOperator.bootstrap( kernelDatabase.getDatabaseId() );
        try
        {
            clusterComponentsLife.init();
            kernelDatabase.init();
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
        finally
        {
            signal.bootstrapped();
        }
    }

    private void ensureRecovered() throws Exception
    {
        recoveryFacade.recovery( kernelDatabase.getDatabaseLayout() );
    }

    @Override
    protected void stop0() throws Exception
    {
        topologyService.onDatabaseStop( kernelDatabase.getDatabaseId() );
        downloadService.stop();
        raftMachine.stopTimers();
        raftMessageHandler.stop();
        applicationProcess.stop();
        kernelDatabase.stop();
        clusterComponentsLife.stop();

        kernelDatabase.shutdown();
        clusterComponentsLife.shutdown();
        removePanicEventHandlers();
    }

    private void addPanicEventHandlers()
    {
        var panicEventHandlers = List.of(
                raiseAvailabilityGuard( kernelDatabase ),
                markUnhealthy( kernelDatabase ),
                applicationProcess,
                raftMachine,
                stopDatabase( kernelDatabase, clusterInternalOperator ) );

        panicService.addPanicEventHandlers( kernelDatabase.getDatabaseId(), panicEventHandlers );
    }

    private void removePanicEventHandlers()
    {
        panicService.removePanicEventHandlers( kernelDatabase.getDatabaseId() );
    }

}
