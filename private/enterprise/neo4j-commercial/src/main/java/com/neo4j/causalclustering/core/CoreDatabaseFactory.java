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
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
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
import com.neo4j.causalclustering.core.state.CoreBootstrapper;
import com.neo4j.causalclustering.core.state.CoreDatabaseLife;
import com.neo4j.causalclustering.core.state.CoreEditionKernelComponents;
import com.neo4j.causalclustering.core.state.CoreKernelResolvers;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.CoreStateStorageFactory;
import com.neo4j.causalclustering.core.state.RaftLogPruner;
import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyMachine;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.id.FreeIdFilteredIdGeneratorFactory;
import com.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import com.neo4j.causalclustering.core.state.machines.id.IdReusabilityCondition;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationStateMachine;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdGeneratorFactory;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdRangeAcquirer;
import com.neo4j.causalclustering.core.state.machines.locks.LeaderOnlyLockManager;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
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
import com.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.helper.TimeoutStrategy;
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
import com.neo4j.kernel.impl.enterprise.id.CommercialIdTypeConfigurationProvider;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.EditionLocksFactories;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.logging.LogProvider;
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

import static com.neo4j.causalclustering.core.CausalClusteringSettings.array_block_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.label_token_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.label_token_name_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.neostore_block_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.node_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.node_labels_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.property_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.property_key_token_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.property_key_token_name_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_frequency;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.relationship_group_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.relationship_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.relationship_type_token_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.relationship_type_token_name_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.schema_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_apply_max_batch_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_flush_window_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.status_throughput_window;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.string_block_id_allocation_size;
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
    private final LogService logService;
    private final LogProvider debugLog;
    private final CompositeDatabaseAvailabilityGuard availabilityGuard;
    private final StorageEngineFactory storageEngineFactory;

    private final ClusteredDatabaseManager databaseManager;
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
    private final Map<IdType,Integer> allocationSizes;
    private final CommercialIdTypeConfigurationProvider idTypeConfigurationProvider;
    private final RecoveryFacade recoveryFacade;
    private final Outbound<AdvertisedSocketAddress,Message> raftSender;

    CoreDatabaseFactory( GlobalModule globalModule, PanicService panicService, ClusteredDatabaseManager databaseManager, CoreTopologyService topologyService,
            CoreStateStorageFactory storageFactory, TemporaryDatabaseFactory temporaryDatabaseFactory, Map<DatabaseId,DatabaseInitializer> databaseInitializers,
            MemberId myIdentity, RaftGroupFactory raftGroupFactory, RaftMessageDispatcher raftMessageDispatcher,
            CatchupComponentsProvider catchupComponentsProvider, RecoveryFacade recoveryFacade, RaftMessageLogger<MemberId> raftLogger,
            Outbound<AdvertisedSocketAddress,Message> raftSender )
    {
        this.config = globalModule.getGlobalConfig();
        this.clock = globalModule.getGlobalClock();
        this.jobScheduler = globalModule.getJobScheduler();
        this.fileSystem = globalModule.getFileSystem();
        this.pageCache = globalModule.getPageCache();
        this.logService = globalModule.getLogService();
        this.debugLog = globalModule.getLogService().getInternalLogProvider();
        this.availabilityGuard = globalModule.getGlobalAvailabilityGuard();
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

        this.allocationSizes = getIdTypeAllocationSizeFromConfig( config );

        this.cursorTracerSupplier = globalModule.getTracers().getPageCursorTracerSupplier();
        this.idTypeConfigurationProvider = new CommercialIdTypeConfigurationProvider( config );
    }

    CoreRaftContext createRaftContext( DatabaseId databaseId, LifeSupport life, Monitors monitors, Dependencies dependencies,
            BootstrapContext bootstrapContext )
    {
        // Must be constructed before storage is touched by other modules.
        StartupCoreStateCheck startupCoreStateCheck = new StartupCoreStateCheck( fileSystem, storageFactory.layout(), databaseId );

        // TODO: Remove this when ID-files are transactional.
        life.add( new IdFilesSanitationModule( startupCoreStateCheck, databaseManager, fileSystem, debugLog ) );

        DatabaseInitializer databaseInitializer = databaseInitializers.getOrDefault( databaseId, NO_INITIALIZATION );
        RaftBinder raftBinder = createRaftBinder(
                databaseId, config, monitors, storageFactory, bootstrapContext, temporaryDatabaseFactory, databaseInitializer );

        CommandIndexTracker commandIndexTracker = dependencies.satisfyDependency( new CommandIndexTracker() );
        initialiseStatusDescriptionEndpoint( dependencies, commandIndexTracker, life );

        long logThresholdMillis = config.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        LoggingOutbound<MemberId,RaftMessage> raftOutbound = new LoggingOutbound<>(
                new RaftOutbound( topologyService, raftSender, raftMessageDispatcher, raftBinder, debugLog, logThresholdMillis, myIdentity, clock ), myIdentity,
                raftLogger );

        RaftGroup raftGroup = raftGroupFactory.create( databaseId, raftOutbound, life, monitors, dependencies );

        GlobalSession myGlobalSession = new GlobalSession( UUID.randomUUID(), myIdentity );
        LocalSessionPool sessionPool = new LocalSessionPool( myGlobalSession );

        ProgressTracker progressTracker = new ProgressTrackerImpl( myGlobalSession );
        RaftReplicator replicator = createReplicator( raftGroup.raftMachine(), sessionPool, progressTracker, monitors, raftOutbound );

        return new CoreRaftContext( raftGroup, replicator, commandIndexTracker, progressTracker, raftBinder );
    }

    CoreEditionKernelComponents createKernelComponents( DatabaseId databaseId, LifeSupport life, CoreRaftContext raftContext,
            CoreKernelResolvers kernelResolvers )
    {
        RaftGroup raftGroup = raftContext.raftGroup();
        Replicator replicator = raftContext.replicator();

        ReplicatedIdAllocationStateMachine idAllocationStateMachine = createIdAllocationStateMachine( databaseId, life );
        DatabaseIdContext idContext = createIdContext( databaseId, raftGroup.raftMachine(), raftContext.commandIndexTracker(), raftContext.replicator(),
                idAllocationStateMachine );

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

        ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine = createLockTokenStateMachine( databaseId, life );

        ReplicatedTokenStateMachine labelTokenStateMachine = new ReplicatedTokenStateMachine( labelTokenRegistry, debugLog, databaseManager );
        ReplicatedTokenStateMachine propertyKeyTokenStateMachine = new ReplicatedTokenStateMachine( propertyKeyTokenRegistry, debugLog, databaseManager );

        ReplicatedTokenStateMachine relationshipTypeTokenStateMachine = new ReplicatedTokenStateMachine(
                relationshipTypeTokenRegistry, debugLog, databaseManager );

        ReplicatedTransactionStateMachine replicatedTxStateMachine = new ReplicatedTransactionStateMachine( raftContext.commandIndexTracker(),
                replicatedLockTokenStateMachine, config.get( state_machine_apply_max_batch_size ), debugLog, cursorTracerSupplier, databaseManager );

        Locks lockManager = createLockManager(
                config, clock, logService, replicator, myIdentity, raftGroup.raftMachine(), replicatedLockTokenStateMachine, databaseId );

        RecoverConsensusLogIndex consensusLogIndexRecovery = new RecoverConsensusLogIndex( kernelResolvers.txIdStore(), kernelResolvers.txStore(), debugLog );

        CoreStateMachines stateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine, relationshipTypeTokenStateMachine,
                propertyKeyTokenStateMachine, replicatedLockTokenStateMachine, idAllocationStateMachine, new DummyMachine(), consensusLogIndexRecovery );

        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );

        CommitProcessFactory commitProcessFactory = new CoreCommitProcessFactory( databaseId, replicator, stateMachines, panicService );

        AccessCapability accessCapability = new LeaderCanWrite( raftGroup.raftMachine() );

        return new CoreEditionKernelComponents( commitProcessFactory, lockManager, tokenHolders, idContext, stateMachines, accessCapability );
    }

    void createDatabase( DatabaseId databaseId, LifeSupport life, Monitors monitors, Dependencies dependencies, StoreDownloadContext downloadContext,
            Database kernelDatabase, CoreEditionKernelComponents kernelComponents, CoreRaftContext raftContext )
    {
        RaftGroup raftGroup = raftContext.raftGroup();

        SessionTracker sessionTracker = createSessionTracker( databaseId, life );

        StateStorage<Long> lastFlushedStateStorage = storageFactory.createLastFlushedStorage( databaseId, life );
        CoreState coreState = new CoreState( sessionTracker, lastFlushedStateStorage, kernelComponents.stateMachines() );

        CommandApplicationProcess commandApplicationProcess = createCommandApplicationProcess( raftGroup, panicService, config, life, jobScheduler,
                dependencies, monitors, raftContext.progressTracker(), sessionTracker, coreState );

        CoreSnapshotService snapshotService = new CoreSnapshotService( commandApplicationProcess, raftGroup.raftLog(), coreState, raftGroup.raftMachine() );
        dependencies.satisfyDependencies( snapshotService );

        CoreDownloaderService downloadService = createDownloader( catchupComponentsProvider, panicService, jobScheduler, monitors, commandApplicationProcess,
                snapshotService, downloadContext );

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
                messageHandler, snapshotService, downloadService, recoveryFacade );

        life.add( coreDatabaseLife );

        panicService.addPanicEventHandler( commandApplicationProcess );
        panicService.addPanicEventHandler( raftGroup.raftMachine() );
    }

    private RaftBinder createRaftBinder( DatabaseId databaseId, Config config, Monitors monitors, CoreStateStorageFactory storageFactory,
            BootstrapContext bootstrapContext, TemporaryDatabaseFactory temporaryDatabaseFactory, DatabaseInitializer databaseInitializer )
    {
        CoreBootstrapper coreBootstrapper = new CoreBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer, fileSystem, config, debugLog,
                pageCache, storageEngineFactory );

        SimpleStorage<RaftId> raftIdStorage = storageFactory.createRaftIdStorage( databaseId );
        int minimumCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
        Duration clusterBindingTimeout = config.get( CausalClusteringSettings.cluster_binding_timeout );
        return new RaftBinder( databaseId, raftIdStorage, topologyService, Clocks.systemClock(), () -> sleep( 100 ), clusterBindingTimeout, coreBootstrapper,
                minimumCoreHosts, monitors );
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
            CoreState coreState )
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

    private SessionTracker createSessionTracker( DatabaseId databaseId, LifeSupport life )
    {
        StateStorage<GlobalSessionTrackerState> sessionTrackerStorage = storageFactory.createSessionTrackerStorage( databaseId, life );
        return new SessionTracker( sessionTrackerStorage );
    }

    private RaftReplicator createReplicator( LeaderLocator leaderLocator, LocalSessionPool sessionPool, ProgressTracker progressTracker, Monitors monitors,
            Outbound<MemberId,RaftMessage> raftOutbound )
    {
        Duration initialBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_base );
        Duration upperBoundBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_limit );

        TimeoutStrategy progressRetryStrategy = new ExponentialBackoffStrategy( initialBackoff, upperBoundBackoff );
        long availabilityTimeoutMillis = config.get( CausalClusteringSettings.replication_retry_timeout_base ).toMillis();

        return new RaftReplicator( leaderLocator, myIdentity, raftOutbound, sessionPool, progressTracker, progressRetryStrategy, availabilityTimeoutMillis,
                availabilityGuard, debugLog, databaseManager, monitors );
    }

    private CoreDownloaderService createDownloader( CatchupComponentsProvider catchupComponentsProvider, Panicker panicService, JobScheduler jobScheduler,
            Monitors monitors, CommandApplicationProcess commandApplicationProcess, CoreSnapshotService snapshotService, StoreDownloadContext downloadContext )
    {
        SnapshotDownloader snapshotDownloader = new SnapshotDownloader( debugLog, catchupComponentsProvider.catchupClientFactory() );
        StoreDownloader storeDownloader = new StoreDownloader( catchupComponentsRepository, debugLog );
        CoreDownloader downloader = new CoreDownloader( snapshotDownloader, storeDownloader, debugLog );
        ExponentialBackoffStrategy backoffStrategy = new ExponentialBackoffStrategy( 1, 30, SECONDS );

        return new CoreDownloaderService( jobScheduler, downloader, downloadContext, snapshotService, commandApplicationProcess, debugLog, backoffStrategy,
                panicService, monitors );
    }

    private DatabaseIdContext createIdContext( DatabaseId databaseId, RaftMachine raftMachine, CommandIndexTracker commandIndexTracker, Replicator replicator,
            ReplicatedIdAllocationStateMachine idAllocationStateMachine )
    {
        ReplicatedIdRangeAcquirer idRangeAcquirer = new ReplicatedIdRangeAcquirer( databaseId, replicator, idAllocationStateMachine, allocationSizes,
                myIdentity, debugLog );
        BooleanSupplier idReuse = new IdReusabilityCondition( commandIndexTracker, raftMachine, myIdentity );
        Function<DatabaseId,IdGeneratorFactory> idGeneratorProvider = id -> createIdGeneratorFactory(
                fileSystem, debugLog, idTypeConfigurationProvider, idRangeAcquirer );
        IdContextFactory idContextFactory = IdContextFactoryBuilder.of( idTypeConfigurationProvider, jobScheduler ).withIdGenerationFactoryProvider(
                idGeneratorProvider ).withFactoryWrapper( generator -> new FreeIdFilteredIdGeneratorFactory( generator, idReuse ) ).build();
        return idContextFactory.createIdContext( databaseId );
    }

    private ReplicatedIdAllocationStateMachine createIdAllocationStateMachine( DatabaseId databaseId, LifeSupport life )
    {
        StateStorage<IdAllocationState> idAllocationStorage = storageFactory.createIdAllocationStorage( databaseId, life );
        return new ReplicatedIdAllocationStateMachine( idAllocationStorage );
    }

    private ReplicatedLockTokenStateMachine createLockTokenStateMachine( DatabaseId databaseId, LifeSupport life )
    {
        StateStorage<ReplicatedLockTokenState> lockTokenStorage = storageFactory.createLockTokenStorage( databaseId, life );
        return new ReplicatedLockTokenStateMachine( lockTokenStorage );
    }

    private Map<IdType,Integer> getIdTypeAllocationSizeFromConfig( Config config )
    {
        Map<IdType,Integer> allocationSizes = new HashMap<>( IdType.values().length );
        allocationSizes.put( IdType.NODE, config.get( node_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP, config.get( relationship_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY, config.get( property_id_allocation_size ) );
        allocationSizes.put( IdType.STRING_BLOCK, config.get( string_block_id_allocation_size ) );
        allocationSizes.put( IdType.ARRAY_BLOCK, config.get( array_block_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY_KEY_TOKEN, config.get( property_key_token_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY_KEY_TOKEN_NAME, config.get( property_key_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_TYPE_TOKEN, config.get( relationship_type_token_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_TYPE_TOKEN_NAME, config.get( relationship_type_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.LABEL_TOKEN, config.get( label_token_id_allocation_size ) );
        allocationSizes.put( IdType.LABEL_TOKEN_NAME, config.get( label_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.NEOSTORE_BLOCK, config.get( neostore_block_id_allocation_size ) );
        allocationSizes.put( IdType.SCHEMA, config.get( schema_id_allocation_size ) );
        allocationSizes.put( IdType.NODE_LABELS, config.get( node_labels_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_GROUP, config.get( relationship_group_id_allocation_size ) );
        return allocationSizes;
    }

    private IdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fileSystem, final LogProvider logProvider,
            IdTypeConfigurationProvider idTypeConfigurationProvider, ReplicatedIdRangeAcquirer idRangeAcquirer )
    {
        return new ReplicatedIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider, idTypeConfigurationProvider, panicService );
    }

    private Locks createLockManager( final Config config, Clock clock, final LogService logging, final Replicator replicator, MemberId myself,
            LeaderLocator leaderLocator, ReplicatedLockTokenStateMachine lockTokenStateMachine, DatabaseId databaseId )
    {
        LocksFactory lockFactory = createLockFactory( config, logging );
        Locks localLocks = EditionLocksFactories.createLockManager( lockFactory, config, clock );
        return new LeaderOnlyLockManager( myself, replicator, leaderLocator, localLocks, lockTokenStateMachine, databaseId );
    }

    private void initialiseStatusDescriptionEndpoint( Dependencies dependencies, CommandIndexTracker commandIndexTracker, LifeSupport life )
    {
        Duration samplingWindow = config.get( status_throughput_window );
        ThroughputMonitor throughputMonitor = new ThroughputMonitor( debugLog, clock, jobScheduler, samplingWindow,
                commandIndexTracker::getAppliedCommandIndex );
        life.add( throughputMonitor );
        dependencies.satisfyDependency( throughputMonitor );
    }
}
