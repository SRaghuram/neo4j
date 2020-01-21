/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.common.DatabaseTopologyNotifier;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.status_throughput_window;
import static java.lang.String.format;

class ReadReplicaDatabaseFactory
{
    private final Config config;
    private final SystemNanoClock clock;
    private final JobScheduler jobScheduler;
    private final TopologyService topologyService;
    private final MemberId myIdentity;
    private final CatchupComponentsRepository catchupComponentsRepository;
    private final CatchupClientFactory catchupClientFactory;
    private final ReplicatedDatabaseEventService databaseEventService;
    private final ClusterStateStorageFactory clusterStateFactory;
    private final PanicService panicService;
    private final DatabaseStartAborter databaseStartAborter;

    ReadReplicaDatabaseFactory( Config config, SystemNanoClock clock, JobScheduler jobScheduler, TopologyService topologyService, MemberId myIdentity,
            CatchupComponentsRepository catchupComponentsRepository,
            CatchupClientFactory catchupClientFactory, ReplicatedDatabaseEventService databaseEventService, ClusterStateStorageFactory clusterStateFactory,
            PanicService panicService, DatabaseStartAborter databaseStartAborter )
    {
        this.config = config;
        this.clock = clock;
        this.jobScheduler = jobScheduler;
        this.topologyService = topologyService;
        this.myIdentity = myIdentity;
        this.catchupComponentsRepository = catchupComponentsRepository;
        this.catchupClientFactory = catchupClientFactory;
        this.databaseEventService = databaseEventService;
        this.panicService = panicService;
        this.clusterStateFactory = clusterStateFactory;
        this.databaseStartAborter = databaseStartAborter;
    }

    ReadReplicaDatabase createDatabase( ReadReplicaDatabaseContext databaseContext, ClusterInternalDbmsOperator clusterInternalOperator )
    {
        NamedDatabaseId namedDatabaseId = databaseContext.databaseId();
        Database kernelDatabase = databaseContext.database();
        DatabaseLogService databaseLogService = kernelDatabase.getLogService();
        DatabaseLogProvider internalLogProvider = databaseLogService.getInternalLogProvider();
        DatabaseLogProvider userLogProvider = databaseLogService.getUserLogProvider();

        LifeSupport clusterComponents = new LifeSupport();
        Executor catchupExecutor = jobScheduler.executor( Group.CATCHUP_CLIENT );
        CommandIndexTracker commandIndexTracker = databaseContext.dependencies().satisfyDependency( new CommandIndexTracker() );
        initialiseStatusDescriptionEndpoint( commandIndexTracker, clusterComponents, databaseContext.dependencies(), internalLogProvider );

        TimerService timerService = new TimerService( jobScheduler, internalLogProvider );
        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, config, internalLogProvider, myIdentity );
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector = createUpstreamDatabaseStrategySelector(
                myIdentity, config, internalLogProvider, topologyService, defaultStrategy );

        DatabasePanicker panicker = panicService.panickerFor( namedDatabaseId );
        ReplicatedDatabaseEventDispatch databaseEventDispatch = databaseEventService.getDatabaseEventDispatch( namedDatabaseId );
        CatchupProcessManager catchupProcess = new CatchupProcessManager( catchupExecutor, catchupComponentsRepository, databaseContext, panicker,
                topologyService, catchupClientFactory, upstreamDatabaseStrategySelector, timerService, commandIndexTracker, internalLogProvider,
                config, databaseEventDispatch );

        var raftIdStorage = clusterStateFactory.createRaftIdStorage( databaseContext.databaseId().name(), internalLogProvider );

        // TODO: Fix lifecycle issue.
        Supplier<CatchupComponents> catchupComponentsSupplier = () -> catchupComponentsRepository.componentsFor( namedDatabaseId ).orElseThrow(
                () -> new IllegalStateException( format( "No per database catchup components exist for database %s.", namedDatabaseId.name() ) ) );

        ReadReplicaBootstrap bootstrap = new ReadReplicaBootstrap( databaseContext, upstreamDatabaseStrategySelector, internalLogProvider,
                userLogProvider, topologyService, catchupComponentsSupplier, clusterInternalOperator, databaseStartAborter );

        RaftIdCheck raftIdCheck = new RaftIdCheck( raftIdStorage, namedDatabaseId );

        DatabaseTopologyNotifier topologyNotifier = new DatabaseTopologyNotifier( namedDatabaseId, topologyService );

        ReadReplicaPanicHandlers panicHandler = new ReadReplicaPanicHandlers( panicService, kernelDatabase, clusterInternalOperator );

        return new ReadReplicaDatabase( catchupProcess, kernelDatabase, clusterComponents, bootstrap, panicHandler, raftIdCheck, topologyNotifier );
    }

    private UpstreamDatabaseStrategySelector createUpstreamDatabaseStrategySelector( MemberId myself, Config config, LogProvider logProvider,
            TopologyService topologyService, ConnectToRandomCoreServerStrategy defaultStrategy )
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

    private void initialiseStatusDescriptionEndpoint( CommandIndexTracker commandIndexTracker, LifeSupport life, Dependencies dependencies,
            DatabaseLogProvider debugLog )
    {
        Duration samplingWindow = config.get( status_throughput_window );
        ThroughputMonitor throughputMonitor = new ThroughputMonitor( debugLog, clock, jobScheduler, samplingWindow,
                commandIndexTracker::getAppliedCommandIndex );
        life.add( throughputMonitor );
        dependencies.satisfyDependency( throughputMonitor );
    }
}
