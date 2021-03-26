/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.dbms.error_handling.PanicService;
import com.neo4j.causalclustering.monitoring.ThroughputMonitorService;
import com.neo4j.causalclustering.readreplica.tx.AsyncTxApplier;
import com.neo4j.causalclustering.readreplica.tx.BatchingTxApplierFactory;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.PreferFollower;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import com.neo4j.dbms.TopologyPublisher;

import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.constant;

class ReadReplicaDatabaseFactory
{
    private final Config config;
    private final JobScheduler jobScheduler;
    private final TopologyService topologyService;
    private final ServerId serverId;
    private final CatchupComponentsRepository catchupComponentsRepository;
    private final ReplicatedDatabaseEventService databaseEventService;
    private final ClusterStateStorageFactory clusterStateFactory;
    private final PanicService panicService;
    private final DatabaseStartAborter databaseStartAborter;
    private final PageCacheTracer pageCacheTracer;
    private final AsyncTxApplier asyncTxApplier;
    private final ClusterSystemGraphDbmsModel systemDbmsModel;

    ReadReplicaDatabaseFactory( Config config, JobScheduler jobScheduler, TopologyService topologyService,
            ServerId serverId, CatchupComponentsRepository catchupComponentsRepository, ReplicatedDatabaseEventService databaseEventService,
            ClusterStateStorageFactory clusterStateFactory,
            PanicService panicService, DatabaseStartAborter databaseStartAborter, PageCacheTracer pageCacheTracer,
            AsyncTxApplier asyncTxApplier, ClusterSystemGraphDbmsModel systemDbmsModel  )
    {
        this.config = config;
        this.jobScheduler = jobScheduler;
        this.topologyService = topologyService;
        this.serverId = serverId;
        this.catchupComponentsRepository = catchupComponentsRepository;
        this.databaseEventService = databaseEventService;
        this.panicService = panicService;
        this.clusterStateFactory = clusterStateFactory;
        this.databaseStartAborter = databaseStartAborter;
        this.pageCacheTracer = pageCacheTracer;
        this.systemDbmsModel = systemDbmsModel;
        this.asyncTxApplier = asyncTxApplier;
    }

    ReadReplicaDatabase createDatabase( ReadReplicaDatabaseContext databaseContext, ClusterInternalDbmsOperator clusterInternalOperator )
    {
        var namedDatabaseId = databaseContext.namedDatabaseId();
        var kernelDatabase = databaseContext.kernelDatabase();
        var databaseLogService = kernelDatabase.getLogService();
        var internalLogProvider = databaseLogService.getInternalLogProvider();
        var userLogProvider = databaseLogService.getUserLogProvider();

        LifeSupport clusterComponents = new LifeSupport();
        CommandIndexTracker commandIndexTracker = databaseContext.dependencies().satisfyDependency( new CommandIndexTracker() );
        initialiseStatusDescriptionEndpoint( commandIndexTracker, clusterComponents, databaseContext.dependencies() );

        UpstreamDatabaseSelectionStrategy defaultStrategy = createDefaultStrategy( internalLogProvider );
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector = createUpstreamDatabaseStrategySelector(
                serverId, config, internalLogProvider, topologyService, defaultStrategy );

        CatchupPollingProcess catchupPollingProcess =
                createCatchupPollingProcess( databaseContext, namedDatabaseId, internalLogProvider, commandIndexTracker, upstreamDatabaseStrategySelector );

        var timerService = new TimerService( jobScheduler, internalLogProvider );
        var txPullInterval = config.get( CausalClusteringSettings.pull_interval );

        var catchupJobScheduler = new CatchupJobScheduler( timerService, catchupPollingProcess, txPullInterval );

        var raftIdStorage = clusterStateFactory.createRaftGroupIdStorage( databaseContext.namedDatabaseId().name(), internalLogProvider );

        // TODO: Fix lifecycle issue.
        Supplier<CatchupComponents> catchupComponentsSupplier = () -> catchupComponentsRepository.componentsFor( namedDatabaseId ).orElseThrow(
                () -> new IllegalStateException( format( "No per database catchup components exist for database %s.", namedDatabaseId.name() ) ) );

        var backoffStrategy = constant( 1, SECONDS );
        var bootstrap = new ReadReplicaBootstrap( databaseContext, upstreamDatabaseStrategySelector, internalLogProvider, userLogProvider, topologyService,
                catchupComponentsSupplier, clusterInternalOperator, databaseStartAborter, backoffStrategy, systemDbmsModel );

        var raftIdCheck = new RaftIdCheck( raftIdStorage, namedDatabaseId );

        var topologyPublisher = TopologyPublisher.from( namedDatabaseId, topologyService::onDatabaseStart, topologyService::onDatabaseStop );

        var panicHandler = new ReadReplicaPanicHandlers( panicService, kernelDatabase, clusterInternalOperator, databaseStartAborter );

        return ReadReplicaDatabase.create( catchupPollingProcess, catchupJobScheduler, kernelDatabase, clusterComponents, bootstrap, panicHandler, raftIdCheck,
                topologyPublisher, databaseStartAborter );
    }

    private CatchupPollingProcess createCatchupPollingProcess( ReadReplicaDatabaseContext databaseContext,
            org.neo4j.kernel.database.NamedDatabaseId namedDatabaseId, org.neo4j.logging.internal.DatabaseLogProvider internalLogProvider,
            CommandIndexTracker commandIndexTracker, UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector )
    {
        var panicker = panicService.panicker();
        var databaseEventDispatch = databaseEventService.getDatabaseEventDispatch( namedDatabaseId );

        var catchupComponentsProvider = new CatchupComponentsProvider( catchupComponentsRepository, namedDatabaseId );

        long applyBatchSize = ByteUnit.mebiBytes( config.get( CausalClusteringInternalSettings.read_replica_transaction_applier_batch_size ) );
        long maxQueueSize = ByteUnit.mebiBytes( config.get( CausalClusteringInternalSettings.read_replica_transaction_applier_max_queue_size ) );

        var batchingTxApplierFactory =
                new BatchingTxApplierFactory( databaseContext, commandIndexTracker, internalLogProvider, databaseEventDispatch, pageCacheTracer,
                        asyncTxApplier );

        var upstreamProvider = new CatchupAddressProvider.UpstreamStrategyBasedAddressProvider( topologyService, upstreamDatabaseStrategySelector );
        var catchupPollingProcess =
                new CatchupPollingProcess( maxQueueSize, applyBatchSize, databaseContext, batchingTxApplierFactory,
                        databaseEventDispatch, internalLogProvider, panicker, upstreamProvider,
                        catchupComponentsProvider );

        databaseContext.dependencies().satisfyDependency( catchupPollingProcess ); //For ReadReplicaToggleProcedure
        return catchupPollingProcess;
    }

    private UpstreamDatabaseSelectionStrategy createDefaultStrategy( DatabaseLogProvider internalLogProvider )
    {
        final var defaultStrategy = new PreferFollower();
        defaultStrategy.inject( topologyService, config, internalLogProvider, serverId );
        return defaultStrategy;
    }

    private UpstreamDatabaseStrategySelector createUpstreamDatabaseStrategySelector( ServerId myself, Config config, LogProvider logProvider,
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

    private void initialiseStatusDescriptionEndpoint( CommandIndexTracker commandIndexTracker, LifeSupport life, Dependencies dependencies )
    {
        var throughputMonitor = dependencies.resolveDependency( ThroughputMonitorService.class ).createMonitor( commandIndexTracker );
        life.add( throughputMonitor );
        dependencies.satisfyDependency( throughputMonitor );
    }
}
