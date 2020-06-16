/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.SessionTracker;
import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.common.DatabaseTopologyNotifier;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftGroup;
import com.neo4j.causalclustering.core.consensus.RaftGroupFactory;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.InboundRaftMessageContainer;
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
import com.neo4j.causalclustering.core.state.BootstrapSaver;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreEditionKernelComponents;
import com.neo4j.causalclustering.core.state.CoreKernelResolvers;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.RaftBootstrapper;
import com.neo4j.causalclustering.core.state.RaftLogPruner;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.machines.StateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyMachine;
import com.neo4j.causalclustering.core.state.machines.lease.ClusterLeaseCoordinator;
import com.neo4j.causalclustering.core.state.machines.lease.LeaderOnlyLockManager;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseStateMachine;
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
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.causalclustering.messaging.LoggingOutbound;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.causalclustering.messaging.RaftOutbound;
import com.neo4j.causalclustering.monitoring.ThroughputMonitorService;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.TypicallyConnectToRandomReadReplicaStrategy;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.graphdb.factory.EditionLocksFactories;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.helpers.ExponentialBackoffStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.TokenRegistry;
import org.neo4j.token.api.TokenHolder;

import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_frequency;
import static com.neo4j.configuration.CausalClusteringSettings.state_machine_apply_max_batch_size;
import static com.neo4j.configuration.CausalClusteringSettings.state_machine_flush_window_size;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockFactory;
import static org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder.defaultIdGeneratorFactoryProvider;

class CoreDatabaseFactory
{
    private static final String ID_CACHE_CLUSTER_CLEANUP_TAG = "idCacheClusterCleanup";
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
    private final ClusterStateStorageFactory storageFactory;

    private final TemporaryDatabaseFactory temporaryDatabaseFactory;

    private final MemberId myIdentity;
    private final RaftGroupFactory raftGroupFactory;
    private final RaftMessageDispatcher raftMessageDispatcher;
    private final RaftMessageLogger<MemberId> raftLogger;

    private final PageCacheTracer pageCacheTracer;
    private final RecoveryFacade recoveryFacade;
    private final Outbound<SocketAddress,RaftMessages.OutboundRaftMessageContainer<?>> raftSender;
    private final ReplicatedDatabaseEventService databaseEventService;
    private final ClusterSystemGraphDbmsModel dbmsModel;
    private final DatabaseStartAborter databaseStartAborter;
    private final BootstrapSaver bootstrapSaver;

    CoreDatabaseFactory( GlobalModule globalModule, PanicService panicService, DatabaseManager<ClusteredDatabaseContext> databaseManager,
            CoreTopologyService topologyService, ClusterStateStorageFactory storageFactory, TemporaryDatabaseFactory temporaryDatabaseFactory,
            MemberId myIdentity, RaftGroupFactory raftGroupFactory,
            RaftMessageDispatcher raftMessageDispatcher, CatchupComponentsProvider catchupComponentsProvider, RecoveryFacade recoveryFacade,
            RaftMessageLogger<MemberId> raftLogger, Outbound<SocketAddress,RaftMessages.OutboundRaftMessageContainer<?>> raftSender,
            ReplicatedDatabaseEventService databaseEventService,
            ClusterSystemGraphDbmsModel dbmsModel, DatabaseStartAborter databaseStartAborter )
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

        this.myIdentity = myIdentity;
        this.raftGroupFactory = raftGroupFactory;
        this.raftMessageDispatcher = raftMessageDispatcher;
        this.catchupComponentsProvider = catchupComponentsProvider;
        this.recoveryFacade = recoveryFacade;
        this.raftLogger = raftLogger;
        this.raftSender = raftSender;
        this.databaseEventService = databaseEventService;
        var tracers = globalModule.getTracers();
        this.pageCacheTracer = tracers.getPageCacheTracer();
        this.dbmsModel = dbmsModel;
        this.databaseStartAborter = databaseStartAborter;
        this.bootstrapSaver = new BootstrapSaver( fileSystem, globalModule.getLogService().getInternalLogProvider() );
    }

    CoreRaftContext createRaftContext( NamedDatabaseId namedDatabaseId, LifeSupport life, Monitors monitors, Dependencies dependencies,
            BootstrapContext bootstrapContext, DatabaseLogService logService, MemoryTracker memoryTracker )
    {
        DatabaseLogProvider debugLog = logService.getInternalLogProvider();

        SimpleStorage<RaftId> raftIdStorage = storageFactory.createRaftIdStorage( namedDatabaseId.name(), debugLog );
        RaftBinder raftBinder = createRaftBinder( namedDatabaseId, config, monitors, raftIdStorage, bootstrapContext,
                temporaryDatabaseFactory, debugLog, dbmsModel, memoryTracker );

        CommandIndexTracker commandIndexTracker = dependencies.satisfyDependency( new CommandIndexTracker() );
        initialiseStatusDescriptionEndpoint( dependencies, commandIndexTracker, life );

        long logThresholdMillis = config.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        LoggingOutbound<MemberId,RaftMessage> raftOutbound = new LoggingOutbound<>(
                new RaftOutbound( topologyService, raftSender, raftMessageDispatcher, raftBinder, debugLog, logThresholdMillis, myIdentity, clock ),
                namedDatabaseId, myIdentity, raftLogger );

        RaftGroup raftGroup = raftGroupFactory.create( namedDatabaseId, raftOutbound, life, monitors, dependencies, logService );

        GlobalSession myGlobalSession = new GlobalSession( UUID.randomUUID(), myIdentity );
        LocalSessionPool sessionPool = new LocalSessionPool( myGlobalSession );

        ProgressTracker progressTracker = new ProgressTrackerImpl( myGlobalSession );
        RaftReplicator replicator =
                createReplicator( namedDatabaseId, raftGroup.raftMachine(), sessionPool, progressTracker, monitors, raftOutbound, debugLog );

        return new CoreRaftContext( raftGroup, replicator, commandIndexTracker, progressTracker, raftBinder, raftIdStorage );
    }

    CoreEditionKernelComponents createKernelComponents( NamedDatabaseId namedDatabaseId, LifeSupport life, CoreRaftContext raftContext,
            CoreKernelResolvers kernelResolvers, DatabaseLogService logService, VersionContextSupplier versionContextSupplier, MemoryTracker memoryTracker )
    {
        RaftGroup raftGroup = raftContext.raftGroup();
        Replicator replicator = raftContext.replicator();
        DatabaseLogProvider debugLog = logService.getInternalLogProvider();

        ReplicatedLeaseStateMachine replicatedLeaseStateMachine = createLeaseStateMachine( namedDatabaseId, life, debugLog, kernelResolvers, pageCacheTracer );

        DatabaseIdContext idContext = createIdContext( namedDatabaseId );

        Supplier<StorageEngine> storageEngineSupplier = kernelResolvers.storageEngine();

        TokenRegistry relationshipTypeTokenRegistry = new TokenRegistry( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder = new ReplicatedRelationshipTypeTokenHolder( namedDatabaseId,
                relationshipTypeTokenRegistry, replicator, idContext.getIdGeneratorFactory(), storageEngineSupplier, pageCacheTracer, memoryTracker );

        TokenRegistry propertyKeyTokenRegistry = new TokenRegistry( TokenHolder.TYPE_PROPERTY_KEY );
        ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder = new ReplicatedPropertyKeyTokenHolder( namedDatabaseId, propertyKeyTokenRegistry, replicator,
                idContext.getIdGeneratorFactory(), storageEngineSupplier, pageCacheTracer, memoryTracker );

        TokenRegistry labelTokenRegistry = new TokenRegistry( TokenHolder.TYPE_LABEL );
        ReplicatedLabelTokenHolder labelTokenHolder = new ReplicatedLabelTokenHolder( namedDatabaseId, labelTokenRegistry, replicator,
                idContext.getIdGeneratorFactory(), storageEngineSupplier, pageCacheTracer, memoryTracker );

        ReplicatedDatabaseEventDispatch databaseEventDispatch = databaseEventService.getDatabaseEventDispatch( namedDatabaseId );
        StateMachineCommitHelper commitHelper = new StateMachineCommitHelper( raftContext.commandIndexTracker(), versionContextSupplier,
                databaseEventDispatch, pageCacheTracer );

        ReplicatedTokenStateMachine labelTokenStateMachine =
                new ReplicatedTokenStateMachine( commitHelper, labelTokenRegistry, debugLog, storageEngineFactory.commandReaderFactory() );
        ReplicatedTokenStateMachine propertyKeyTokenStateMachine =
                new ReplicatedTokenStateMachine( commitHelper, propertyKeyTokenRegistry, debugLog, storageEngineFactory.commandReaderFactory() );
        ReplicatedTokenStateMachine relTypeTokenStateMachine =
                new ReplicatedTokenStateMachine( commitHelper, relationshipTypeTokenRegistry, debugLog, storageEngineFactory.commandReaderFactory() );

        ReplicatedTransactionStateMachine replicatedTxStateMachine = new ReplicatedTransactionStateMachine( commitHelper, replicatedLeaseStateMachine,
                config.get( state_machine_apply_max_batch_size ), debugLog, storageEngineFactory.commandReaderFactory() );

        Locks lockManager = createLockManager( config, clock, logService );

        RecoverConsensusLogIndex consensusLogIndexRecovery = new RecoverConsensusLogIndex( kernelResolvers.txIdStore(), kernelResolvers.txStore(), debugLog );

        CoreStateMachines stateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine, relTypeTokenStateMachine,
                propertyKeyTokenStateMachine, replicatedLeaseStateMachine, new DummyMachine(), consensusLogIndexRecovery );

        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );

        ClusterLeaseCoordinator leaseCoordinator = new ClusterLeaseCoordinator(
                myIdentity, replicator, raftGroup.raftMachine(), replicatedLeaseStateMachine, namedDatabaseId );

        CommitProcessFactory commitProcessFactory = new CoreCommitProcessFactory( namedDatabaseId, replicator, stateMachines, leaseCoordinator );

        AccessCapabilityFactory accessCapabilityFactory = AccessCapabilityFactory.fixed( new LeaderCanWrite( raftGroup.raftMachine() ) );

        return new CoreEditionKernelComponents( commitProcessFactory, lockManager, tokenHolders, idContext, stateMachines, accessCapabilityFactory,
                leaseCoordinator );
    }

    CoreDatabase createDatabase( NamedDatabaseId namedDatabaseId, LifeSupport clusterComponents, Monitors monitors, Dependencies dependencies,
            StoreDownloadContext downloadContext, Database kernelDatabase, CoreEditionKernelComponents kernelComponents, CoreRaftContext raftContext,
            ClusterInternalDbmsOperator internalOperator )
    {
        DatabasePanicker panicker = panicService.panickerFor( namedDatabaseId );
        RaftGroup raftGroup = raftContext.raftGroup();
        DatabaseLogProvider debugLog = kernelDatabase.getInternalLogProvider();

        SessionTracker sessionTracker = createSessionTracker( namedDatabaseId, clusterComponents, debugLog );

        StateStorage<Long> lastFlushedStateStorage = storageFactory.createLastFlushedStorage( namedDatabaseId.name(), clusterComponents, debugLog );
        CoreState coreState = new CoreState( sessionTracker, lastFlushedStateStorage, kernelComponents.stateMachines() );

        CommandApplicationProcess applicationProcess = createCommandApplicationProcess( raftGroup, panicker, config, clusterComponents, jobScheduler,
                dependencies, monitors, raftContext.progressTracker(), sessionTracker, coreState, debugLog );

        CoreSnapshotService snapshotService = new CoreSnapshotService( applicationProcess, raftGroup.raftLog(), coreState,
                raftGroup.raftMachine(), namedDatabaseId, kernelDatabase.getLogService(), clock );
        dependencies.satisfyDependencies( snapshotService );

        CoreDownloaderService downloadService = createDownloader( catchupComponentsProvider, panicker, jobScheduler, monitors, applicationProcess,
                snapshotService, downloadContext, debugLog );

        TypicallyConnectToRandomReadReplicaStrategy defaultStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        defaultStrategy.inject( topologyService, config, debugLog, myIdentity );

        UpstreamDatabaseStrategySelector catchupStrategySelector = createUpstreamDatabaseStrategySelector(
                myIdentity, config, debugLog, topologyService, defaultStrategy );

        LeaderProvider leaderProvider = new LeaderProvider( raftGroup.raftMachine(), topologyService );

        CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider =
                new CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider( leaderProvider, topologyService, catchupStrategySelector );

        dependencies.satisfyDependency( raftGroup.raftMachine() );

        raftGroup.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStateStorage::getInitialState );

        RaftMessageHandlerChainFactory raftMessageHandlerChainFactory = new RaftMessageHandlerChainFactory( jobScheduler, clock, debugLog, monitors, config,
                raftMessageDispatcher, catchupAddressProvider, panicker );

        LifecycleMessageHandler<InboundRaftMessageContainer<?>> messageHandler = raftMessageHandlerChainFactory.createMessageHandlerChain(
                raftGroup, downloadService, applicationProcess );

        DatabaseTopologyNotifier topologyNotifier = new DatabaseTopologyNotifier( namedDatabaseId, topologyService );

        CorePanicHandlers panicHandler = new CorePanicHandlers( raftGroup.raftMachine(), kernelDatabase, applicationProcess, internalOperator, panicService );

        TempBootstrapDir tempBootstrapDir = new TempBootstrapDir( fileSystem, kernelDatabase.getDatabaseLayout() );
        CoreBootstrap bootstrap = new CoreBootstrap( kernelDatabase, raftContext.raftBinder(), messageHandler, snapshotService, downloadService,
                internalOperator, databaseStartAborter, raftContext.raftIdStorage(), bootstrapSaver, tempBootstrapDir );

        return new CoreDatabase( raftGroup.raftMachine(), kernelDatabase, applicationProcess, messageHandler, downloadService, recoveryFacade,
                clusterComponents, panicHandler, bootstrap, topologyNotifier );
    }

    private RaftBinder createRaftBinder( NamedDatabaseId namedDatabaseId, Config config, Monitors monitors, SimpleStorage<RaftId> raftIdStorage,
            BootstrapContext bootstrapContext, TemporaryDatabaseFactory temporaryDatabaseFactory,
            DatabaseLogProvider debugLog, ClusterSystemGraphDbmsModel systemGraph, MemoryTracker memoryTracker )
    {
        var pageCache = new DatabasePageCache( this.pageCache, EmptyVersionContextSupplier.EMPTY );
        var raftBootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, pageCache, fileSystem, debugLog,
                storageEngineFactory, config, bootstrapSaver, pageCacheTracer, memoryTracker );

        var minimumCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
        var refuseToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
        var clusterBindingTimeout = config.get( CausalClusteringInternalSettings.cluster_binding_timeout );
        return new RaftBinder( namedDatabaseId, myIdentity, raftIdStorage, topologyService, systemGraph, Clocks.systemClock(), () -> sleep( 100 ),
                clusterBindingTimeout, raftBootstrapper, minimumCoreHosts, refuseToBeLeader, monitors );
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

    private CommandApplicationProcess createCommandApplicationProcess( RaftGroup raftGroup, DatabasePanicker panicker, Config config, LifeSupport life,
            JobScheduler jobScheduler, Dependencies dependencies, Monitors monitors, ProgressTracker progressTracker, SessionTracker sessionTracker,
            CoreState coreState, DatabaseLogProvider debugLog )
    {
        CommandApplicationProcess commandApplicationProcess = new CommandApplicationProcess( raftGroup.raftLog(),
                config.get( state_machine_apply_max_batch_size ), config.get( state_machine_flush_window_size ), debugLog, progressTracker, sessionTracker,
                coreState, raftGroup.inFlightCache(), monitors, panicker, jobScheduler );

        dependencies.satisfyDependency( commandApplicationProcess ); // lastApplied() for CC-robustness

        RaftLogPruner raftLogPruner = new RaftLogPruner( raftGroup.raftMachine(), commandApplicationProcess );
        dependencies.satisfyDependency( raftLogPruner );

        life.add( new PruningScheduler( raftLogPruner, jobScheduler, config.get( raft_log_pruning_frequency ).toMillis(), debugLog ) );

        return commandApplicationProcess;
    }

    private SessionTracker createSessionTracker( NamedDatabaseId namedDatabaseId, LifeSupport life, DatabaseLogProvider databaseLogProvider )
    {
        StateStorage<GlobalSessionTrackerState> sessionTrackerStorage = storageFactory.createSessionTrackerStorage(
                namedDatabaseId.name(), life, databaseLogProvider );
        return new SessionTracker( sessionTrackerStorage );
    }

    private RaftReplicator createReplicator( NamedDatabaseId namedDatabaseId, LeaderLocator leaderLocator, LocalSessionPool sessionPool,
            ProgressTracker progressTracker, Monitors monitors, Outbound<MemberId,RaftMessage> raftOutbound, DatabaseLogProvider debugLog )
    {
        Duration initialBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_base );
        Duration upperBoundBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_limit );

        TimeoutStrategy progressRetryStrategy = new ExponentialBackoffStrategy( initialBackoff, upperBoundBackoff );
        long availabilityTimeoutMillis = config.get( CausalClusteringSettings.replication_retry_timeout_base ).toMillis();

        Duration leaderAwaitDuration = config.get( CausalClusteringSettings.replication_leader_await_timeout );

        return new RaftReplicator( namedDatabaseId, leaderLocator, myIdentity, raftOutbound, sessionPool, progressTracker, progressRetryStrategy,
                availabilityTimeoutMillis, debugLog, databaseManager, monitors, leaderAwaitDuration );
    }

    private CoreDownloaderService createDownloader( CatchupComponentsProvider catchupComponentsProvider, DatabasePanicker panicker, JobScheduler jobScheduler,
            Monitors monitors, CommandApplicationProcess commandApplicationProcess, CoreSnapshotService snapshotService, StoreDownloadContext downloadContext,
            DatabaseLogProvider debugLog )
    {
        SnapshotDownloader snapshotDownloader = new SnapshotDownloader( debugLog, catchupComponentsProvider.catchupClientFactory() );
        StoreDownloader storeDownloader = new StoreDownloader( catchupComponentsRepository, debugLog );
        CoreDownloader downloader = new CoreDownloader( snapshotDownloader, storeDownloader, debugLog );
        ExponentialBackoffStrategy backoffStrategy = new ExponentialBackoffStrategy( 1, 30, SECONDS );

        return new CoreDownloaderService( jobScheduler, downloader, downloadContext, snapshotService, databaseEventService, commandApplicationProcess, debugLog,
                backoffStrategy, panicker, monitors, databaseStartAborter );
    }

    private DatabaseIdContext createIdContext( NamedDatabaseId namedDatabaseId )
    {
        Function<NamedDatabaseId,IdGeneratorFactory> idGeneratorProvider = defaultIdGeneratorFactoryProvider( fileSystem, config );
        IdContextFactory idContextFactory = IdContextFactoryBuilder
                .of( fileSystem, jobScheduler, config, pageCacheTracer )
                .withIdGenerationFactoryProvider( idGeneratorProvider )
                .withFactoryWrapper( generator -> generator )
                .build();
        return idContextFactory.createIdContext( namedDatabaseId );
    }

    private ReplicatedLeaseStateMachine createLeaseStateMachine( NamedDatabaseId namedDatabaseId, LifeSupport life,
            DatabaseLogProvider databaseLogProvider, CoreKernelResolvers resolvers, PageCacheTracer pageCacheTracer )
    {
        StateStorage<ReplicatedLeaseState> leaseStorage = storageFactory.createLeaseStorage( namedDatabaseId.name(), life, databaseLogProvider );
        return new ReplicatedLeaseStateMachine( leaseStorage, () ->
        {
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( ID_CACHE_CLUSTER_CLEANUP_TAG ) )
            {
                resolvers.idGeneratorFactory().get().clearCache( cursorTracer );
            }
        } );
    }

    private Locks createLockManager( final Config config, SystemNanoClock clock, LogService logService )
    {
        LocksFactory lockFactory = createLockFactory( config, logService );
        Locks localLocks = EditionLocksFactories.createLockManager( lockFactory, config, clock );
        return new LeaderOnlyLockManager( localLocks );
    }

    private void initialiseStatusDescriptionEndpoint( Dependencies dependencies, CommandIndexTracker commandIndexTracker, LifeSupport life )
    {
        var throughputMonitor = dependencies.resolveDependency( ThroughputMonitorService.class ).createMonitor( commandIndexTracker );
        life.add( throughputMonitor );
        dependencies.satisfyDependency( throughputMonitor );
    }
}
