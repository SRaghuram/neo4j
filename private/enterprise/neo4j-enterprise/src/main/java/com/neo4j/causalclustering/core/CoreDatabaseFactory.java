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
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.core.consensus.log.pruning.PruningScheduler;
import com.neo4j.causalclustering.core.replication.ClusterStatusResponseCollector;
import com.neo4j.causalclustering.core.replication.ClusterStatusService;
import com.neo4j.causalclustering.core.replication.ProgressTracker;
import com.neo4j.causalclustering.core.replication.ProgressTrackerImpl;
import com.neo4j.causalclustering.core.replication.RaftReplicator;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
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
import com.neo4j.causalclustering.core.state.machines.NoOperationStateMachine;
import com.neo4j.causalclustering.core.state.machines.StateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.lease.ClusterLeaseCoordinator;
import com.neo4j.causalclustering.core.state.machines.lease.LeaderOnlyLockManager;
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
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.RaftCoreTopologyConnector;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.identity.CoreIdentityModule;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;
import com.neo4j.causalclustering.messaging.LoggingOutbound;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.causalclustering.messaging.RaftOutbound;
import com.neo4j.causalclustering.monitoring.ThroughputMonitorService;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.TypicallyConnectToRandomReadReplicaStrategy;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import com.neo4j.dbms.database.DbmsLogEntryWriterProvider;

import java.util.UUID;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.graphdb.factory.EditionLocksFactories;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.TokenRegistry;
import org.neo4j.token.api.TokenHolder;

import static com.neo4j.configuration.CausalClusteringInternalSettings.cluster_status_request_maximum_wait;
import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_frequency;
import static com.neo4j.configuration.CausalClusteringSettings.state_machine_apply_max_batch_size;
import static com.neo4j.configuration.CausalClusteringSettings.state_machine_flush_window_size;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.function.Suppliers.lazySingleton;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockFactory;
import static org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder.defaultIdGeneratorFactoryProvider;
import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.exponential;
import static org.neo4j.scheduler.Group.CLUSTER_STATUS_CHECK_SERVICE;

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

    private final CoreIdentityModule identityModule;
    private final RaftGroupFactory raftGroupFactory;
    private final RaftMessageDispatcher raftMessageDispatcher;
    private final RaftMessageLogger<RaftMemberId> raftLogger;

    private final PageCacheTracer pageCacheTracer;
    private final RecoveryFacade recoveryFacade;
    private final Outbound<SocketAddress,RaftMessages.OutboundRaftMessageContainer<?>> raftSender;
    private final ReplicatedDatabaseEventService databaseEventService;
    private final ClusterSystemGraphDbmsModel dbmsModel;
    private final DatabaseStartAborter databaseStartAborter;
    private final BootstrapSaver bootstrapSaver;
    private final DbmsLogEntryWriterProvider dbmsLogEntryWriterProvider;
    private final ReadOnlyDatabaseChecker readOnlyDatabaseChecker;

    CoreDatabaseFactory( GlobalModule globalModule, PanicService panicService, DatabaseManager<ClusteredDatabaseContext> databaseManager,
                         CoreTopologyService topologyService, ClusterStateStorageFactory storageFactory, TemporaryDatabaseFactory temporaryDatabaseFactory,
                         CoreIdentityModule identityModule, RaftGroupFactory raftGroupFactory,
                         RaftMessageDispatcher raftMessageDispatcher, CatchupComponentsProvider catchupComponentsProvider, RecoveryFacade recoveryFacade,
                         RaftMessageLogger<RaftMemberId> raftLogger, Outbound<SocketAddress,RaftMessages.OutboundRaftMessageContainer<?>> raftSender,
                         ReplicatedDatabaseEventService databaseEventService, ClusterSystemGraphDbmsModel dbmsModel,
                         DatabaseStartAborter databaseStartAborter, DbmsLogEntryWriterProvider dbmsLogEntryWriterProvider )
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

        this.identityModule = identityModule;
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
        this.dbmsLogEntryWriterProvider = dbmsLogEntryWriterProvider;
        this.readOnlyDatabaseChecker = new ReadOnlyDatabaseChecker.Default( config );
    }

    CoreRaftContext createRaftContext( NamedDatabaseId namedDatabaseId, LifeSupport raftComponents, Monitors monitors, Dependencies dependencies,
            BootstrapContext bootstrapContext, DatabaseLogService logService, MemoryTracker memoryTracker )
    {
        var raftMemberId = lazySingleton( () -> identityModule.raftMemberId( namedDatabaseId ) );
        var debugLog = logService.getInternalLogProvider();

        var raftGroupIdStorage = storageFactory.createRaftGroupIdStorage( namedDatabaseId.name(), debugLog );
        var raftBinder = createRaftBinder( namedDatabaseId, config, monitors, raftGroupIdStorage, bootstrapContext, temporaryDatabaseFactory, debugLog,
                dbmsModel, memoryTracker, identityModule );

        var commandIndexTracker = dependencies.satisfyDependency( new CommandIndexTracker() );
        initialiseStatusDescriptionEndpoint( dependencies, commandIndexTracker, raftComponents );

        var logThresholdMillis = config.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        var raftOutbound = new LoggingOutbound<>(
                new RaftOutbound( topologyService, raftSender, raftMessageDispatcher, raftBinder, debugLog, logThresholdMillis, raftMemberId, clock ),
                namedDatabaseId, raftLogger );

        var statusResponseCollector = new ClusterStatusResponseCollector();

        var raftGroup = raftGroupFactory.create( namedDatabaseId, raftMemberId, raftOutbound, raftComponents, monitors, dependencies, logService,
                statusResponseCollector, readOnlyDatabaseChecker );

        var sessionId = UUID.randomUUID();
        GlobalSession myGlobalSession = new GlobalSession( sessionId, raftMemberId );
        LocalSessionPool sessionPool = new LocalSessionPool( myGlobalSession );

        var progressTracker = new ProgressTrackerImpl( myGlobalSession );
        var replicator =
                createReplicator( namedDatabaseId, raftGroup.raftMachine(), sessionPool, progressTracker, monitors, raftOutbound, debugLog, raftMemberId );

        var clusterStatusService = new ClusterStatusService( namedDatabaseId, raftMemberId, raftGroup.raftMachine(),
                                                             replicator, logService, statusResponseCollector,
                                                             jobScheduler.executor( CLUSTER_STATUS_CHECK_SERVICE ),
                                                             config.get( cluster_status_request_maximum_wait ) );
        dependencies.satisfyDependency( clusterStatusService ); // Used in ClusterStatusRequestIT

        return new CoreRaftContext( raftGroup, replicator, commandIndexTracker, progressTracker, raftBinder, raftGroupIdStorage );
    }

    CoreEditionKernelComponents createKernelComponents( NamedDatabaseId namedDatabaseId, LifeSupport raftComponents, CoreRaftContext raftContext,
            CoreKernelResolvers kernelResolvers, DatabaseLogService logService,
            VersionContextSupplier versionContextSupplier )
    {
        var logEntryWriterFactory = this.dbmsLogEntryWriterProvider.getEntryWriterFactory( namedDatabaseId );
        var raftGroup = raftContext.raftGroup();
        var replicator = raftContext.replicator();
        var debugLog = logService.getInternalLogProvider();

        var replicatedLeaseStateMachine = createLeaseStateMachine(
                namedDatabaseId, raftComponents, debugLog, kernelResolvers, pageCacheTracer );

        var idContext = createIdContext( namedDatabaseId );

        var storageEngineSupplier = kernelResolvers.storageEngine();

        var relationshipTypeTokenRegistry = new TokenRegistry( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        var relationshipTypeTokenHolder = new ReplicatedRelationshipTypeTokenHolder( namedDatabaseId, relationshipTypeTokenRegistry, replicator,
                                                                                     idContext.getIdGeneratorFactory(), storageEngineSupplier, pageCacheTracer,
                                                                                     logEntryWriterFactory, readOnlyDatabaseChecker );

        var propertyKeyTokenRegistry = new TokenRegistry( TokenHolder.TYPE_PROPERTY_KEY );
        var propertyKeyTokenHolder = new ReplicatedPropertyKeyTokenHolder( namedDatabaseId, propertyKeyTokenRegistry, replicator,
                                                                                                        idContext.getIdGeneratorFactory(),
                                                                                                        storageEngineSupplier, pageCacheTracer,
                                                                                                        logEntryWriterFactory, readOnlyDatabaseChecker );

        var labelTokenRegistry = new TokenRegistry( TokenHolder.TYPE_LABEL );
        var labelTokenHolder = new ReplicatedLabelTokenHolder( namedDatabaseId, labelTokenRegistry, replicator,
                                                               idContext.getIdGeneratorFactory(), storageEngineSupplier, pageCacheTracer,
                                                               logEntryWriterFactory, readOnlyDatabaseChecker );

        var databaseEventDispatch = databaseEventService.getDatabaseEventDispatch( namedDatabaseId );
        var commitHelper = new StateMachineCommitHelper( raftContext.commandIndexTracker(), versionContextSupplier,
                                                         databaseEventDispatch, pageCacheTracer );

        var labelTokenStateMachine = new ReplicatedTokenStateMachine( commitHelper, labelTokenRegistry, debugLog, storageEngineFactory.commandReaderFactory() );
        var propertyKeyTokenStateMachine = new ReplicatedTokenStateMachine( commitHelper, propertyKeyTokenRegistry, debugLog,
                                                                            storageEngineFactory.commandReaderFactory() );
        var relTypeTokenStateMachine = new ReplicatedTokenStateMachine( commitHelper, relationshipTypeTokenRegistry, debugLog,
                                                                        storageEngineFactory.commandReaderFactory() );

        var replicatedTxStateMachine = new ReplicatedTransactionStateMachine( commitHelper, replicatedLeaseStateMachine,
                                                                              config.get( state_machine_apply_max_batch_size ),
                                                                              debugLog, storageEngineFactory.commandReaderFactory() );

        var lockManager = createLockManager( config, clock, logService );

        var consensusLogIndexRecovery = new RecoverConsensusLogIndex( kernelResolvers.txIdStore(), kernelResolvers.txStore(), debugLog );

        var stateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine, relTypeTokenStateMachine,
                                                                 propertyKeyTokenStateMachine, replicatedLeaseStateMachine,
                                                                 consensusLogIndexRecovery, new NoOperationStateMachine<>() );

        var tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );

        var leaseCoordinator = new ClusterLeaseCoordinator( () -> identityModule.raftMemberId( namedDatabaseId.databaseId() ), replicator,
                                                            raftGroup.raftMachine(), replicatedLeaseStateMachine, namedDatabaseId );

        var commitProcessFactory = new CoreCommitProcessFactory( replicator, stateMachines, leaseCoordinator,
                                                                 dbmsLogEntryWriterProvider.getEntryWriterFactory( namedDatabaseId ) );

        var accessCapabilityFactory = AccessCapabilityFactory.fixed( new LeaderCanWrite( raftGroup.raftMachine() ) );

        return new CoreEditionKernelComponents( commitProcessFactory, lockManager, tokenHolders, idContext, stateMachines, accessCapabilityFactory,
                                                leaseCoordinator );
    }

    CoreDatabase createDatabase( NamedDatabaseId namedDatabaseId, LifeSupport raftComponents, Monitors monitors, Dependencies dependencies,
                                 StoreDownloadContext downloadContext, Database kernelDatabase, CoreEditionKernelComponents kernelComponents,
                                 CoreRaftContext raftContext, ClusterInternalDbmsOperator internalOperator )
    {
        var myIdentity = this.identityModule.serverId();
        var panicker = panicService.panicker();
        var databasePanicker = panicService.panickerFor( namedDatabaseId );
        var raftGroup = raftContext.raftGroup();
        var debugLog = kernelDatabase.getInternalLogProvider();

        var sessionTracker = createSessionTracker( namedDatabaseId, raftComponents, debugLog );

        var lastFlushedStateStorage = storageFactory.createLastFlushedStorage( namedDatabaseId.name(), raftComponents, debugLog );
        var coreState = new CoreState( sessionTracker, lastFlushedStateStorage, kernelComponents.stateMachines() );

        var applicationProcess = createCommandApplicationProcess( raftGroup, databasePanicker, config, raftComponents, jobScheduler,
                                                                  dependencies, monitors, raftContext.progressTracker(), sessionTracker,
                                                                  coreState, debugLog );

        var snapshotService = new CoreSnapshotService( applicationProcess, raftGroup.raftLog(), coreState,
                                                       raftGroup.raftMachine(), namedDatabaseId, kernelDatabase.getLogService(), clock );
        dependencies.satisfyDependencies( snapshotService );

        var downloadService = createDownloader( catchupComponentsProvider, panicker, jobScheduler, monitors, applicationProcess,
                                                snapshotService, downloadContext, debugLog );

        var defaultStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        defaultStrategy.inject( topologyService, config, debugLog, myIdentity );

        var catchupStrategySelector = createUpstreamDatabaseStrategySelector( myIdentity, config, debugLog, topologyService, defaultStrategy );

        var leaderProvider = new LeaderProvider( raftGroup.raftMachine(), topologyService );

        var catchupAddressProvider =
                new CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider( leaderProvider, topologyService, catchupStrategySelector );

        dependencies.satisfyDependency( raftGroup.raftMachine() );

        raftGroup.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStateStorage::getInitialState );

        var raftMessageHandlerChainFactory = new RaftMessageHandlerChainFactory( jobScheduler, clock, debugLog, monitors, config,
                                                                                 raftMessageDispatcher, catchupAddressProvider, databasePanicker );

        var messageHandler = raftMessageHandlerChainFactory.createMessageHandlerChain( raftGroup, downloadService, applicationProcess );

        var topologyComponents = new LifeSupport();
        topologyComponents.add( new DatabaseTopologyNotifier( namedDatabaseId, topologyService ) );
        topologyComponents.add( new RaftCoreTopologyConnector( topologyService, raftContext.raftGroup().raftMachine(), namedDatabaseId ) );

        var panicHandler = new CorePanicHandlers( raftGroup.raftMachine(), kernelDatabase, applicationProcess, internalOperator, panicService );

        var tempBootstrapDir = new TempBootstrapDir( fileSystem, kernelDatabase.getDatabaseLayout() );
        var raftStarter = new RaftStarter( kernelDatabase, raftContext.raftBinder(), messageHandler, snapshotService, downloadService, internalOperator,
                databaseStartAborter, raftContext.raftIdStorage(), bootstrapSaver, tempBootstrapDir, raftComponents );

        return new CoreDatabase( raftGroup.raftMachine(), kernelDatabase, applicationProcess, messageHandler, downloadService, recoveryFacade,
                                 panicHandler, raftStarter, topologyComponents );
    }

    private RaftBinder createRaftBinder( NamedDatabaseId namedDatabaseId, Config config, Monitors monitors, SimpleStorage<RaftGroupId> raftIdStorage,
                                         BootstrapContext bootstrapContext, TemporaryDatabaseFactory temporaryDatabaseFactory,
                                         DatabaseLogProvider debugLog, ClusterSystemGraphDbmsModel systemGraph, MemoryTracker memoryTracker,
                                         CoreIdentityModule myIdentity )
    {
        var pageCache = new DatabasePageCache( this.pageCache, EmptyVersionContextSupplier.EMPTY, namedDatabaseId.name() );
        var raftBootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, pageCache, fileSystem, debugLog,
                                                     storageEngineFactory, config, bootstrapSaver, pageCacheTracer, memoryTracker );

        var minimumCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
        var refuseToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
        var clusterBindingTimeout = config.get( CausalClusteringSettings.cluster_binding_timeout );
        return new RaftBinder( namedDatabaseId, myIdentity, raftIdStorage, topologyService, systemGraph, clock, () -> sleep( 100 ),
                               clusterBindingTimeout, raftBootstrapper, minimumCoreHosts, refuseToBeLeader, monitors );
    }

    private UpstreamDatabaseStrategySelector createUpstreamDatabaseStrategySelector( ServerId myself, Config config, LogProvider logProvider,
                                                                                     TopologyService topologyService,
                                                                                     UpstreamDatabaseSelectionStrategy defaultStrategy )
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
                                                                       JobScheduler jobScheduler, Dependencies dependencies, Monitors monitors,
                                                                       ProgressTracker progressTracker, SessionTracker sessionTracker,
                                                                       CoreState coreState, DatabaseLogProvider debugLog )
    {

        var commandApplicationProcess = new CommandApplicationProcess( raftGroup.raftLog(), config.get( state_machine_apply_max_batch_size ),
                                                                       config.get( state_machine_flush_window_size ), debugLog, progressTracker,
                                                                       sessionTracker, coreState, raftGroup.inFlightCache(), monitors, panicker, jobScheduler );

        dependencies.satisfyDependency( commandApplicationProcess ); // lastApplied() for CC-robustness

        var raftLogPruner = new RaftLogPruner( raftGroup.raftMachine(), commandApplicationProcess );
        dependencies.satisfyDependency( raftLogPruner );

        life.add( new PruningScheduler( raftLogPruner, jobScheduler, config.get( raft_log_pruning_frequency ).toMillis(), debugLog ) );

        return commandApplicationProcess;
    }

    private SessionTracker createSessionTracker( NamedDatabaseId namedDatabaseId, LifeSupport life, DatabaseLogProvider databaseLogProvider )
    {
        var sessionTrackerStorage = storageFactory.createSessionTrackerStorage( namedDatabaseId.name(), life, databaseLogProvider );
        return new SessionTracker( sessionTrackerStorage );
    }

    private RaftReplicator createReplicator( NamedDatabaseId namedDatabaseId, LeaderLocator leaderLocator, LocalSessionPool sessionPool,
                                             ProgressTracker progressTracker, Monitors monitors, Outbound<RaftMemberId,RaftMessage> raftOutbound,
                                             DatabaseLogProvider debugLog, Lazy<RaftMemberId> myIdentity )
    {
        var initialBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_base );
        var upperBoundBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_limit );
        var progressRetryStrategy = exponential( initialBackoff.toMillis(), upperBoundBackoff.toMillis(), MILLISECONDS );
        var availabilityTimeoutMillis = config.get( CausalClusteringSettings.replication_retry_timeout_base ).toMillis();

        var leaderAwaitDuration = config.get( CausalClusteringSettings.replication_leader_await_timeout );

        return new RaftReplicator( namedDatabaseId, leaderLocator, myIdentity, raftOutbound, sessionPool, progressTracker, progressRetryStrategy,
                                   availabilityTimeoutMillis, debugLog, databaseManager, monitors, leaderAwaitDuration );
    }

    private CoreDownloaderService createDownloader( CatchupComponentsProvider catchupComponentsProvider, Panicker panicker, JobScheduler jobScheduler,
                                                    Monitors monitors, CommandApplicationProcess commandApplicationProcess, CoreSnapshotService snapshotService,
                                                    StoreDownloadContext downloadContext,
                                                    DatabaseLogProvider debugLog )
    {
        var snapshotDownloader = new SnapshotDownloader( debugLog, catchupComponentsProvider.catchupClientFactory() );
        var storeDownloader = new StoreDownloader( catchupComponentsRepository, debugLog );
        var downloader = new CoreDownloader( snapshotDownloader, storeDownloader );
        var exponential = exponential( 1, 30, SECONDS );

        return new CoreDownloaderService( jobScheduler, downloader, downloadContext, snapshotService, databaseEventService, commandApplicationProcess, debugLog,
                                          exponential, panicker, monitors, databaseStartAborter );
    }

    private DatabaseIdContext createIdContext( NamedDatabaseId namedDatabaseId )
    {
        var idGeneratorProvider = defaultIdGeneratorFactoryProvider( fileSystem, config );
        var idContextFactory = IdContextFactoryBuilder
                .of( fileSystem, jobScheduler, config, pageCacheTracer )
                .withIdGenerationFactoryProvider( idGeneratorProvider )
                .withFactoryWrapper( generator -> generator )
                .build();
        return idContextFactory.createIdContext( namedDatabaseId );
    }

    private ReplicatedLeaseStateMachine createLeaseStateMachine( NamedDatabaseId namedDatabaseId, LifeSupport raftComponents,
                                                                 DatabaseLogProvider databaseLogProvider, CoreKernelResolvers resolvers,
                                                                 PageCacheTracer pageCacheTracer )
    {
        var leaseStorage = storageFactory.createLeaseStorage( namedDatabaseId.name(), raftComponents, databaseLogProvider );
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
        var lockFactory = createLockFactory( config, logService );
        var localLocks = EditionLocksFactories.createLockManager( lockFactory, config, clock );
        return new LeaderOnlyLockManager( localLocks );
    }

    private void initialiseStatusDescriptionEndpoint( Dependencies dependencies, CommandIndexTracker commandIndexTracker, LifeSupport raftComponents )
    {
        var throughputMonitor = dependencies.resolveDependency( ThroughputMonitorService.class ).createMonitor( commandIndexTracker );
        raftComponents.add( throughputMonitor );
        dependencies.satisfyDependency( throughputMonitor );
    }
}
