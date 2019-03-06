/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.server;

import com.neo4j.causalclustering.ReplicationModule;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.CatchupServersModule;
import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.IdentityModule;
import com.neo4j.causalclustering.core.consensus.ConsensusModule;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.pruning.PruningScheduler;
import com.neo4j.causalclustering.core.consensus.membership.MembershipWaiter;
import com.neo4j.causalclustering.core.consensus.membership.MembershipWaiterLifecycle;
import com.neo4j.causalclustering.core.state.ClusteringModule;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreLife;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.CoreStateService;
import com.neo4j.causalclustering.core.state.RaftLogPruner;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloader;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.core.state.snapshot.SnapshotDownloader;
import com.neo4j.causalclustering.core.state.snapshot.StoreDownloader;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.CompositeSuspendable;
import com.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.common.Dependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.recovery.RecoveryRequiredChecker;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CoreServerModule extends CatchupServersModule
{
    public final MembershipWaiterLifecycle membershipWaiterLifecycle;
    private final IdentityModule identityModule;
    private final ConsensusModule consensusModule;
    private final ClusteringModule clusteringModule;
    private final Supplier<DatabaseHealth> dbHealthSupplier;
    private final CommandApplicationProcess commandApplicationProcess;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final JobScheduler jobScheduler;
    private final GlobalModule globalModule;

    public CoreServerModule( IdentityModule identityModule, final GlobalModule globalModule, ConsensusModule consensusModule,
            CoreStateService coreStateService, ClusteringModule clusteringModule, ReplicationModule replicationModule, DatabaseService databaseService,
            Supplier<DatabaseHealth> dbHealthSupplier, PipelineBuilders pipelineBuilders, InstalledProtocolHandler installedProtocolsHandler,
            CatchupHandlerFactory handlerFactory, String databaseName, Panicker panicker )
    {
        super( databaseService, pipelineBuilders, globalModule, databaseName );
        this.identityModule = identityModule;
        this.consensusModule = consensusModule;
        this.clusteringModule = clusteringModule;
        this.dbHealthSupplier = dbHealthSupplier;
        this.globalModule = globalModule;

        this.jobScheduler = globalModule.getJobScheduler();

        final Dependencies globalDependencies = globalModule.getGlobalDependencies();
        final Monitors globalMonitors = globalModule.getGlobalMonitors();
        CompositeSuspendable suspendOnStoreCopy = new CompositeSuspendable();

        commandApplicationProcess = new CommandApplicationProcess(
                consensusModule.raftLog(),
                globalConfig.get( CausalClusteringSettings.state_machine_apply_max_batch_size ),
                globalConfig.get( CausalClusteringSettings.state_machine_flush_window_size ),
                logProvider,
                replicationModule.getProgressTracker(),
                replicationModule.getSessionTracker(), coreStateService,
                consensusModule.inFlightCache(), globalMonitors,
                panicker );

        globalDependencies.satisfyDependency( commandApplicationProcess ); // lastApplied() for CC-robustness

        this.snapshotService = new CoreSnapshotService( commandApplicationProcess, coreStateService,
                consensusModule.raftLog(), consensusModule.raftMachine() );

        SnapshotDownloader snapshotDownloader = new SnapshotDownloader( logProvider, catchupClientFactory );
        StoreDownloader storeDownloader = new StoreDownloader( catchupComponents(), logProvider );
        CoreDownloader downloader = new CoreDownloader( snapshotDownloader, storeDownloader, logProvider );
        ExponentialBackoffStrategy backoffStrategy = new ExponentialBackoffStrategy( 1, 30, SECONDS );

        this.downloadService = new CoreDownloaderService( jobScheduler, downloader, snapshotService, suspendOnStoreCopy, databaseService,
                commandApplicationProcess, logProvider, backoffStrategy, panicker, globalMonitors );

        this.membershipWaiterLifecycle = createMembershipWaiterLifecycle();

        CatchupServerHandler catchupServerHandler = handlerFactory.create( snapshotService );
        catchupServer = createCatchupServer( installedProtocolsHandler, catchupServerHandler, databaseName );
        backupServer = createBackupServer( installedProtocolsHandler, catchupServerHandler, databaseName );

        RaftLogPruner raftLogPruner = new RaftLogPruner( consensusModule.raftMachine(), commandApplicationProcess );
        globalDependencies.satisfyDependency( raftLogPruner );

        globalLife.add( new PruningScheduler( raftLogPruner, jobScheduler,
                globalConfig.get( CausalClusteringSettings.raft_log_pruning_frequency ).toMillis(), logProvider ) );

        suspendOnStoreCopy.add( this.catchupServer );
        backupServer.ifPresent( suspendOnStoreCopy::add );
    }

    private MembershipWaiterLifecycle createMembershipWaiterLifecycle()
    {
        long electionTimeout = globalConfig.get( CausalClusteringSettings.leader_election_timeout ).toMillis();
        MembershipWaiter membershipWaiter = new MembershipWaiter( identityModule.myself(), jobScheduler,
                dbHealthSupplier, electionTimeout * 4, logProvider, globalModule.getGlobalMonitors() );
        long joinCatchupTimeout = globalConfig.get( CausalClusteringSettings.join_catch_up_timeout ).toMillis();
        return new MembershipWaiterLifecycle( membershipWaiter, joinCatchupTimeout, consensusModule.raftMachine(), logProvider );
    }

    public Server catchupServer()
    {
        return catchupServer;
    }

    public Optional<Server> backupServer()
    {
        return backupServer;
    }

    public CoreLife createCoreLife( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> handler, LogProvider logProvider,
            RecoveryRequiredChecker recoveryChecker )
    {
        return new CoreLife( consensusModule.raftMachine(), databaseService, clusteringModule.clusterBinder(),
                commandApplicationProcess, handler, snapshotService, downloadService, logProvider, recoveryChecker );
    }

    public CommandApplicationProcess commandApplicationProcess()
    {
        return commandApplicationProcess;
    }

    public CoreDownloaderService downloadService()
    {
        return downloadService;
    }

}
