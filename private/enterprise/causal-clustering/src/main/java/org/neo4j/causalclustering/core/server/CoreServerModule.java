/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.server;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.ReplicationModule;
import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.catchup.CatchupServersModule;
import org.neo4j.causalclustering.common.PipelineBuilders;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.MemberIdRepository;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.pruning.PruningScheduler;
import org.neo4j.causalclustering.core.consensus.membership.MembershipWaiter;
import org.neo4j.causalclustering.core.consensus.membership.MembershipWaiterLifecycle;
import org.neo4j.causalclustering.core.state.ClusteringModule;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.core.state.CoreLife;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.RaftLogPruner;
import org.neo4j.causalclustering.core.state.CoreStateService;
import org.neo4j.causalclustering.core.state.snapshot.CoreDownloader;
import org.neo4j.causalclustering.core.state.snapshot.SnapshotDownloader;
import org.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import org.neo4j.causalclustering.core.state.snapshot.StoreDownloader;
import org.neo4j.causalclustering.helper.CompositeSuspendable;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.causalclustering.net.InstalledProtocolHandler;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CoreServerModule extends CatchupServersModule
{
    public final MembershipWaiterLifecycle membershipWaiterLifecycle;
    private final MemberIdRepository memberIdRepository;
    private final ConsensusModule consensusModule;
    private final ClusteringModule clusteringModule;
    private final Supplier<DatabaseHealth> dbHealthSupplier;
    private final CommandApplicationProcess commandApplicationProcess;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final JobScheduler jobScheduler;
    private final PlatformModule platformModule;

    public CoreServerModule( MemberIdRepository memberIdRepository, final PlatformModule platformModule, ConsensusModule consensusModule,
            CoreStateService coreStateService, ClusteringModule clusteringModule, ReplicationModule replicationModule, DatabaseService databaseService,
            Supplier<DatabaseHealth> dbHealthSupplier, PipelineBuilders pipelineBuilders, InstalledProtocolHandler installedProtocolsHandler,
            CatchupHandlerFactory handlerFactory, String activeDatabaseName )
    {
        super( databaseService, pipelineBuilders, platformModule );
        this.memberIdRepository = memberIdRepository;
        this.consensusModule = consensusModule;
        this.clusteringModule = clusteringModule;
        this.dbHealthSupplier = dbHealthSupplier;
        this.platformModule = platformModule;

        this.jobScheduler = platformModule.jobScheduler;

        final Dependencies dependencies = platformModule.dependencies;
        CompositeSuspendable suspendOnStoreCopy = new CompositeSuspendable();

        commandApplicationProcess = new CommandApplicationProcess(
                consensusModule.raftLog(),
                config.get( CausalClusteringSettings.state_machine_apply_max_batch_size ),
                config.get( CausalClusteringSettings.state_machine_flush_window_size ),
                dbHealthSupplier,
                logProvider,
                replicationModule.getProgressTracker(),
                replicationModule.getSessionTracker(), coreStateService,
                consensusModule.inFlightCache(),
                platformModule.monitors );

        platformModule.dependencies.satisfyDependency( commandApplicationProcess ); // lastApplied() for CC-robustness

        this.snapshotService = new CoreSnapshotService( commandApplicationProcess, coreStateService,
                consensusModule.raftLog(), consensusModule.raftMachine() );

        SnapshotDownloader snapshotDownloader = new SnapshotDownloader( logProvider, catchupClientFactory );
        StoreDownloader storeDownloader = new StoreDownloader( catchupComponents(), logProvider );
        CoreDownloader downloader = new CoreDownloader( databaseService, snapshotDownloader, storeDownloader, logProvider );
        ExponentialBackoffStrategy backoffStrategy = new ExponentialBackoffStrategy( 1, 30, SECONDS );

        this.downloadService = new CoreDownloaderService( platformModule.jobScheduler, downloader, snapshotService, suspendOnStoreCopy, databaseService,
                commandApplicationProcess, logProvider, backoffStrategy, dbHealthSupplier, platformModule.monitors );

        this.membershipWaiterLifecycle = createMembershipWaiterLifecycle();

        CatchupServerHandler catchupServerHandler = handlerFactory.create( snapshotService );
        catchupServer = createCatchupServer( installedProtocolsHandler, catchupServerHandler, activeDatabaseName );
        backupServer = createBackupServer( installedProtocolsHandler, catchupServerHandler, activeDatabaseName );

        RaftLogPruner raftLogPruner = new RaftLogPruner( consensusModule.raftMachine(), commandApplicationProcess, platformModule.clock );
        dependencies.satisfyDependency( raftLogPruner );

        lifeSupport.add( new PruningScheduler( raftLogPruner, jobScheduler,
                config.get( CausalClusteringSettings.raft_log_pruning_frequency ).toMillis(), logProvider ) );

        suspendOnStoreCopy.add( this.catchupServer );
        backupServer.ifPresent( suspendOnStoreCopy::add );
    }

    private MembershipWaiterLifecycle createMembershipWaiterLifecycle()
    {
        long electionTimeout = config.get( CausalClusteringSettings.leader_election_timeout ).toMillis();
        MembershipWaiter membershipWaiter = new MembershipWaiter( memberIdRepository.myself(), jobScheduler,
                dbHealthSupplier, electionTimeout * 4, logProvider, platformModule.monitors );
        long joinCatchupTimeout = config.get( CausalClusteringSettings.join_catch_up_timeout ).toMillis();
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
