/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.SessionTracker;
import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftGroup;
import com.neo4j.causalclustering.core.consensus.RaftGroupFactory;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.core.consensus.log.pruning.PruningScheduler;
import com.neo4j.causalclustering.core.replication.ProgressTracker;
import com.neo4j.causalclustering.core.replication.ProgressTrackerImpl;
import com.neo4j.causalclustering.core.replication.RaftReplicator;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import com.neo4j.causalclustering.core.state.BootstrapContext;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreDatabaseLife;
import com.neo4j.causalclustering.core.state.CoreEditionKernelComponents;
import com.neo4j.causalclustering.core.state.CoreKernelResolvers;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.CoreStateStorageFactory;
import com.neo4j.causalclustering.core.state.RaftBootstrapper;
import com.neo4j.causalclustering.core.state.RaftLogPruner;
import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.machines.StateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.barrier.BarrierState;
import com.neo4j.causalclustering.core.state.machines.barrier.LeaderOnlyLockManager;
import com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenState;
import com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenStateMachine;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyMachine;
import com.neo4j.causalclustering.core.state.machines.id.BarrierAwareIdGeneratorFactory;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.id.FreeIdFilteredIdGeneratorFactory;
import com.neo4j.causalclustering.core.state.machines.id.IdReusabilityCondition;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedLabelTokenHolder;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedPropertyKeyTokenHolder;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedRelationshipTypeTokenHolder;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenStateMachine;
import com.neo4j.causalclustering.core.state.machines.tx.RecoverConsensusLogIndex;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloader;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.core.state.snapshot.SnapshotDownloader;
import com.neo4j.causalclustering.core.state.snapshot.StoreDownloadContext;
import com.neo4j.causalclustering.core.state.snapshot.StoreDownloader;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.causalclustering.messaging.LoggingOutbound;
import com.neo4j.causalclustering.messaging.Message;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.causalclustering.messaging.RaftOutbound;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.TypicallyConnectToRandomReadReplicaStrategy;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.TransactionEventService;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.EditionLocksFactories;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.ExponentialBackoffStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.TokenRegistry;
import org.neo4j.token.api.TokenHolder;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_frequency;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_apply_max_batch_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_flush_window_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.status_throughput_window;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockFactory;
import static org.neo4j.graphdb.factory.module.DatabaseInitializer.NO_INITIALIZATION;

class CoreDatabaseFactory
{
    private final Config config;
    private final SystemNanoClock clock;
    private final JobScheduler jobScheduler;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final StorageEngineFactory storageEngineFactory;

    private final DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private final CatchupComponentsRepository catchupComponentsRepository;
    private final CatchupComponentsProvider catchupComponentsProvider;

    private final PanicService panicService;
    private final CoreTopologyService topologyService;
    private final CoreStateStorageFactory storageFactory;

    private final TemporaryDatabaseFactory temporaryDatabaseFactory;
    private final Map<DatabaseId,DatabaseInitializer> databaseInitializers;

    private final MemberId myIdentity;
    private final RaftGroupFactory raftGroupFactory;
    private final RaftMessageDispatcher raftMessageDispatcher;
    private final RaftMessageLogger<MemberId> raftLogger;

    private final PageCursorTracerSupplier cursorTracerSupplier;
    private final RecoveryFacade recoveryFacade;
    private final Outbound<SocketAddress,Message> raftSender;
    private final TransactionEventService txEventService;

    CoreDatabaseFactory( GlobalModule globalModule, PanicService panicService, DatabaseManager<ClusteredDatabaseContext> databaseManager,
            CoreTopologyService topologyService, CoreStateStorageFactory storageFactory, TemporaryDatabaseFactory temporaryDatabaseFactory,
            Map<DatabaseId,DatabaseInitializer> databaseInitializers, MemberId myIdentity, RaftGroupFactory raftGroupFactory,
            RaftMessageDispatcher raftMessageDispatcher, CatchupComponentsProvider catchupComponentsProvider, RecoveryFacade recoveryFacade,
            RaftMessageLogger<MemberId> raftLogger, Outbound<SocketAddress,Message> raftSender, TransactionEventService txEventService )
    {
        this.config = globalModule.getGlobalConfig();
        this.clock = globalModule.getGlobalClock();
        this.jobScheduler = globalModule.getJobScheduler();
        this.fileSystem = globalModule.getFileSystem();
        this.pageCache = globalModule.getPageCache();
        this.storageEngineFactory = globalModule.getStorageEngineFactory();

        this.databaseManager = databaseManager;
        this.catchupComponentsRepository = new CatchupComponentsRepository( databaseManager );

        this.panicService = panicService;
        this.topologyService = topologyService;
        this.storageFactory = storageFactory;
        this.temporaryDatabaseFactory = temporaryDatabaseFactory;
        this.databaseInitializers = databaseInitializers;

        this.myIdentity = myIdentity;
        this.raftGroupFactory = raftGroupFactory;
        this.raftMessageDispatcher = raftMessageDispatcher;
        this.catchupComponentsProvider = catchupComponentsProvider;
        this.recoveryFacade = recoveryFacade;
        this.raftLogger = raftLogger;
        this.raftSender = raftSender;
        this.txEventService = txEventService;

        this.cursorTracerSupplier = globalModule.getTracers().getPageCursorTracerSupplier();
    }

    CoreRaftContext createRaftContext( DatabaseId databaseId, LifeSupport life, Monitors monitors, Dependencies dependencies,
            BootstrapContext bootstrapContext, DatabaseLogService logService )
    {
        DatabaseLogProvider debugLog = logService.getInternalLogProvider();

        DatabaseInitializer databaseInitializer = databaseInitializers.getOrDefault( databaseId, NO_INITIALIZATION );
        RaftBinder raftBinder = createRaftBinder(
                databaseId, config, monitors, storageFactory, bootstrapContext, temporaryDatabaseFactory, databaseInitializer, debugLog );

        CommandIndexTracker commandIndexTracker = dependencies.satisfyDependency( new CommandIndexTracker() );
        initialiseStatusDescriptionEndpoint( dependencies, commandIndexTracker, life, debugLog );

        long logThresholdMillis = config.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        LoggingOutbound<MemberId,RaftMessage> raftOutbound = new LoggingOutbound<>(
                new RaftOutbound( topologyService, raftSender, raftMessageDispatcher, raftBinder, debugLog, logThresholdMillis, myIdentity, clock ), myIdentity,
                raftLogger );

        RaftGroup raftGroup = raftGroupFactory.create( databaseId, raftOutbound, life, monitors, dependencies, logService );

        GlobalSession myGlobalSession = new GlobalSession( UUID.randomUUID(), myIdentity );
        LocalSessionPool sessionPool = new LocalSessionPool( myGlobalSession );

        ProgressTracker progressTracker = new ProgressTrackerImpl( myGlobalSession );
        RaftReplicator replicator = createReplicator( databaseId, raftGroup.raftMachine(), sessionPool, progressTracker, monitors, raftOutbound, debugLog );

        return new CoreRaftContext( raftGroup, replicator, commandIndexTracker, progressTracker, raftBinder );
    }

    CoreEditionKernelComponents createKernelComponents( DatabaseId databaseId, LifeSupport life, CoreRaftContext raftContext,
            CoreKernelResolvers kernelResolvers, DatabaseLogService logService, VersionContextSupplier versionContextSupplier )
    {
        RaftGroup raftGroup = raftContext.raftGroup();
        Replicator replicator = raftContext.replicator();
        DatabaseLogProvider debugLog = logService.getInternalLogProvider();

        ReplicatedBarrierTokenStateMachine replicatedBarrierTokenStateMachine = createBarrierTokenStateMachine( databaseId, life, debugLog );
        BarrierState barrierState = new BarrierState( myIdentity, replicator, raftGroup.raftMachine(), replicatedBarrierTokenStateMachine, databaseId );

        DatabaseIdContext idContext = createIdContext( databaseId, raftGroup.raftMachine(), raftContext.commandIndexTracker(), barrierState );

        Supplier<StorageEngine> storageEngineSupplier = kernelResolvers.storageEngine();

        TokenRegistry relationshipTypeTokenRegistry = new TokenRegistry( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder = new ReplicatedRelationshipTypeTokenHolder( databaseId,
                relationshipTypeTokenRegistry, replicator, idContext.getIdGeneratorFactory(), storageEngineSupplier );

        TokenRegistry propertyKeyTokenRegistry = new TokenRegistry( TokenHolder.TYPE_PROPERTY_KEY );
        ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder = new ReplicatedPropertyKeyTokenHolder( databaseId, propertyKeyTokenRegistry, replicator,
                idContext.getIdGeneratorFactory(), storageEngineSupplier );

        TokenRegistry labelTokenRegistry = new TokenRegistry( TokenHolder.TYPE_LABEL );
        ReplicatedLabelTokenHolder labelTokenHolder = new ReplicatedLabelTokenHolder( databaseId, labelTokenRegistry, replicator,
                idContext.getIdGeneratorFactory(), storageEngineSupplier );

        StateMachineCommitHelper commitHelper = new StateMachineCommitHelper( raftContext.commandIndexTracker(), cursorTracerSupplier, versionContextSupplier,
                txEventService.getCommitNotifier( databaseId ) );

        ReplicatedTokenStateMachine labelTokenStateMachine = new ReplicatedTokenStateMachine( commitHelper, labelTokenRegistry, debugLog );
        ReplicatedTokenStateMachine propertyKeyTokenStateMachine = new ReplicatedTokenStateMachine( commitHelper, propertyKeyTokenRegistry, debugLog );
        ReplicatedTokenStateMachine relTypeTokenStateMachine = new ReplicatedTokenStateMachine( commitHelper, relationshipTypeTokenRegistry, debugLog );

        ReplicatedTransactionStateMachine replicatedTxStateMachine = new ReplicatedTransactionStateMachine( commitHelper, replicatedBarrierTokenStateMachine,
                config.get( state_machine_apply_max_batch_size ), debugLog );

        Locks lockManager = createLockManager( config, clock, logService,barrierState );

        RecoverConsensusLogIndex consensusLogIndexRecovery = new RecoverConsensusLogIndex( kernelResolvers.txIdStore(), kernelResolvers.txStore(), debugLog );

        CoreStateMachines stateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine, relTypeTokenStateMachine,
                propertyKeyTokenStateMachine, replicatedBarrierTokenStateMachine, new DummyMachine(), consensusLogIndexRecovery );

        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );

        CommitProcessFactory commitProcessFactory = new CoreCommitProcessFactory( databaseId, replicator, stateMachines, panicService );

        AccessCapability accessCapability = new LeaderCanWrite( raftGroup.raftMachine() );

        return new CoreEditionKernelComponents( commitProcessFactory, lockManager, tokenHolders, idContext, stateMachines );
    }

    CoreDatabaseLife createDatabase( DatabaseId databaseId, LifeSupport life, Monitors monitors, Dependencies dependencies,
            StoreDownloadContext downloadContext, Database kernelDatabase, CoreEditionKernelComponents kernelComponents, CoreRaftContext raftContext,
            ClusterInternalDbmsOperator internalOperator )
    {
        RaftGroup raftGroup = raftContext.raftGroup();
        DatabaseLogProvider debugLog = kernelDatabase.getInternalLogProvider();

        SessionTracker sessionTracker = createSessionTracker( databaseId, life, debugLog );

        StateStorage<Long> lastFlushedStateStorage = storageFactory.createLastFlushedStorage( databaseId, life, debugLog );
        CoreState coreState = new CoreState( sessionTracker, lastFlushedStateStorage, kernelComponents.stateMachines() );

        CommandApplicationProcess commandApplicationProcess = createCommandApplicationProcess( raftGroup, panicService, config, life, jobScheduler,
                dependencies, monitors, raftContext.progressTracker(), sessionTracker, coreState, debugLog );

        CoreSnapshotService snapshotService = new CoreSnapshotService( commandApplicationProcess, raftGroup.raftLog(), coreState, raftGroup.raftMachine() );
        dependencies.satisfyDependencies( snapshotService );

        CoreDownloaderService downloadService = createDownloader( catchupComponentsProvider, panicService, jobScheduler, monitors, commandApplicationProcess,
                snapshotService, downloadContext, debugLog );

        TypicallyConnectToRandomReadReplicaStrategy defaultStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        defaultStrategy.inject( topologyService, config, debugLog, myIdentity );

        UpstreamDatabaseStrategySelector catchupStrategySelector = createUpstreamDatabaseStrategySelector(
                myIdentity, config, debugLog, topologyService, defaultStrategy );

        CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider =
                new CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider( raftGroup.raftMachine(), topologyService, catchupStrategySelector );

        dependencies.satisfyDependency( raftGroup.raftMachine() );

        raftGroup.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStateStorage::getInitialState );

        RaftMessageHandlerChainFactory raftMessageHandlerChainFactory = new RaftMessageHandlerChainFactory( jobScheduler, clock, debugLog, monitors, config,
                raftMessageDispatcher, catchupAddressProvider, panicService );

        LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> messageHandler = raftMessageHandlerChainFactory.createMessageHandlerChain(
                raftGroup, downloadService, commandApplicationProcess );

        CoreDatabaseLife coreDatabaseLife = new CoreDatabaseLife( raftGroup.raftMachine(), kernelDatabase, raftContext.raftBinder(), commandApplicationProcess,
                messageHandler, snapshotService, downloadService, recoveryFacade, life, internalOperator, topologyService );

        panicService.addPanicEventHandler( commandApplicationProcess );
        panicService.addPanicEventHandler( raftGroup.raftMachine() );

        return coreDatabaseLife;
    }

    private RaftBinder createRaftBinder( DatabaseId databaseId, Config config, Monitors monitors, CoreStateStorageFactory storageFactory,
            BootstrapContext bootstrapContext, TemporaryDatabaseFactory temporaryDatabaseFactory, DatabaseInitializer databaseInitializer,
            DatabaseLogProvider debugLog )
    {
        var raftBootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer, pageCache, fileSystem,
                debugLog, storageEngineFactory, config );

        SimpleStorage<RaftId> raftIdStorage = storageFactory.createRaftIdStorage( databaseId, debugLog );
        int minimumCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
        Duration clusterBindingTimeout = config.get( CausalClusteringSettings.cluster_binding_timeout );
        return new RaftBinder( databaseId, raftIdStorage, topologyService, Clocks.systemClock(), () -> sleep( 100 ), clusterBindingTimeout,
                raftBootstrapper, minimumCoreHosts, monitors );
    }

    private UpstreamDatabaseStrategySelector createUpstreamDatabaseStrategySelector( MemberId myself, Config config, LogProvider logProvider,
            TopologyService topologyService, UpstreamDatabaseSelectionStrategy defaultStrategy )
    {
        UpstreamDatabaseStrategiesLoader loader;
        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            loader = new UpstreamDatabaseStrategiesLoader( topologyService, config, myself, logProvider );
            logProvider.getLog( getClass() ).info( "Multi-Data Center option enabled." );
        }
        else
        {
            loader = new NoOpUpstreamDatabaseStrategiesLoader();
        }

        return new UpstreamDatabaseStrategySelector( defaultStrategy, loader, logProvider );
    }

    private CommandApplicationProcess createCommandApplicationProcess( RaftGroup raftGroup, Panicker panicker, Config config, LifeSupport life,
            JobScheduler jobScheduler, Dependencies dependencies, Monitors monitors, ProgressTracker progressTracker, SessionTracker sessionTracker,
            CoreState coreState, DatabaseLogProvider debugLog )
    {
        CommandApplicationProcess commandApplicationProcess = new CommandApplicationProcess( raftGroup.raftLog(),
                config.get( state_machine_apply_max_batch_size ), config.get( state_machine_flush_window_size ), debugLog, progressTracker,
                sessionTracker, coreState, raftGroup.inFlightCache(), monitors, panicker );

        dependencies.satisfyDependency( commandApplicationProcess ); // lastApplied() for CC-robustness

        RaftLogPruner raftLogPruner = new RaftLogPruner( raftGroup.raftMachine(), commandApplicationProcess );
        dependencies.satisfyDependency( raftLogPruner );

        life.add( new PruningScheduler( raftLogPruner, jobScheduler, config.get( raft_log_pruning_frequency ).toMillis(), debugLog ) );

        return commandApplicationProcess;
    }

    private SessionTracker createSessionTracker( DatabaseId databaseId, LifeSupport life, DatabaseLogProvider databaseLogProvider )
    {
        StateStorage<GlobalSessionTrackerState> sessionTrackerStorage = storageFactory.createSessionTrackerStorage( databaseId, life, databaseLogProvider );
        return new SessionTracker( sessionTrackerStorage );
    }

    private RaftReplicator createReplicator( DatabaseId databaseId, LeaderLocator leaderLocator, LocalSessionPool sessionPool,
            ProgressTracker progressTracker, Monitors monitors, Outbound<MemberId,RaftMessage> raftOutbound, DatabaseLogProvider debugLog )
    {
        Duration initialBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_base );
        Duration upperBoundBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_limit );

        TimeoutStrategy progressRetryStrategy = new ExponentialBackoffStrategy( initialBackoff, upperBoundBackoff );
        long availabilityTimeoutMillis = config.get( CausalClusteringSettings.replication_retry_timeout_base ).toMillis();

        Duration leaderAwaitDuration = config.get( CausalClusteringSettings.replication_leader_await_timeout );

        return new RaftReplicator( databaseId, leaderLocator, myIdentity, raftOutbound, sessionPool, progressTracker, progressRetryStrategy,
                availabilityTimeoutMillis, debugLog, databaseManager, monitors, leaderAwaitDuration );
    }

    private CoreDownloaderService createDownloader( CatchupComponentsProvider catchupComponentsProvider, Panicker panicService, JobScheduler jobScheduler,
            Monitors monitors, CommandApplicationProcess commandApplicationProcess, CoreSnapshotService snapshotService, StoreDownloadContext downloadContext,
            DatabaseLogProvider debugLog )
    {
        SnapshotDownloader snapshotDownloader = new SnapshotDownloader( debugLog, catchupComponentsProvider.catchupClientFactory() );
        StoreDownloader storeDownloader = new StoreDownloader( catchupComponentsRepository, debugLog );
        CoreDownloader downloader = new CoreDownloader( snapshotDownloader, storeDownloader, debugLog );
        ExponentialBackoffStrategy backoffStrategy = new ExponentialBackoffStrategy( 1, 30, SECONDS );

        return new CoreDownloaderService( jobScheduler, downloader, downloadContext, snapshotService, commandApplicationProcess, debugLog, backoffStrategy,
                panicService, monitors );
    }

    private DatabaseIdContext createIdContext( DatabaseId databaseId, RaftMachine raftMachine, CommandIndexTracker commandIndexTracker,
                                               BarrierState barrierTokenState )
    {
        BooleanSupplier idReuse = new IdReusabilityCondition( commandIndexTracker, raftMachine, myIdentity );
        Function<DatabaseId,IdGeneratorFactory> idGeneratorProvider = id -> createIdGeneratorFactory( databaseId, barrierTokenState );
        IdContextFactory idContextFactory = IdContextFactoryBuilder.of( jobScheduler ).withIdGenerationFactoryProvider(
                idGeneratorProvider ).withFactoryWrapper( generator -> new FreeIdFilteredIdGeneratorFactory( generator, idReuse ) ).build();
        return idContextFactory.createIdContext( databaseId );
    }

    private ReplicatedBarrierTokenStateMachine createBarrierTokenStateMachine( DatabaseId databaseId, LifeSupport life,
                                                                               DatabaseLogProvider databaseLogProvider )
    {
        StateStorage<ReplicatedBarrierTokenState> barrierTokenStorage = storageFactory.createBarrierTokenStorage( databaseId, life, databaseLogProvider );
        return new ReplicatedBarrierTokenStateMachine( barrierTokenStorage );
    }

    private IdGeneratorFactory createIdGeneratorFactory( DatabaseId databaseId, BarrierState barrierTokenState )
    {
        DefaultIdGeneratorFactory real = new DefaultIdGeneratorFactory( fileSystem, RecoveryCleanupWorkCollector.immediate() );
        return new BarrierAwareIdGeneratorFactory( real, databaseId, barrierTokenState );
    }

    private Locks createLockManager( final Config config, Clock clock, final LogService logging, BarrierState barrierTokenState )
    {
        LocksFactory lockFactory = createLockFactory( config );
        Locks localLocks = EditionLocksFactories.createLockManager( lockFactory, config, clock );
        return new LeaderOnlyLockManager( localLocks, barrierTokenState );
    }

    private void initialiseStatusDescriptionEndpoint( Dependencies dependencies, CommandIndexTracker commandIndexTracker, LifeSupport life,
            DatabaseLogProvider debugLog )
    {
        Duration samplingWindow = config.get( status_throughput_window );
        ThroughputMonitor throughputMonitor = new ThroughputMonitor( debugLog, clock, jobScheduler, samplingWindow,
                commandIndexTracker::getAppliedCommandIndex );
        life.add( throughputMonitor );
        dependencies.satisfyDependency( throughputMonitor );
    }
}
