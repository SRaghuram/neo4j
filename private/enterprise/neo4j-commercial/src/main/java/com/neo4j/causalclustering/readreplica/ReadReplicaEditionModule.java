/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;
import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.DefaultDiscoveryMember;
import com.neo4j.causalclustering.discovery.DiscoveryMember;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.ResolutionResolverFactory;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import com.neo4j.causalclustering.error_handling.PanicEventHandlers;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import com.neo4j.dbms.InternalOperator;
import com.neo4j.dbms.LocalOperator;
import com.neo4j.dbms.OperatorConnector;
import com.neo4j.dbms.OperatorState;
import com.neo4j.dbms.ReconcilingDatabaseOperator;
import com.neo4j.dbms.SystemOperator;
import com.neo4j.kernel.enterprise.api.security.provider.CommercialNoAuthSecurityProvider;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.server.security.enterprise.CommercialSecurityModule;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.CompositeDatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.procedure.commercial.builtin.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.procedure.commercial.builtin.EnterpriseBuiltInProcedures;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.status_throughput_window;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Commercial Read Replica edition.
 */
public class ReadReplicaEditionModule extends ClusteringEditionModule
{
    protected final LogProvider logProvider;
    private final DatabaseId defaultDatabaseId;
    private final Config globaConfig;
    private final CompositeDatabaseHealth globalHealth;
    private final GlobalModule globalModule;

    private final MemberId myself;
    private final JobScheduler jobScheduler;
    private final PanicService panicService;
    private final CatchupComponentsProvider catchupComponentsProvider;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final SslPolicyLoader sslPolicyLoader;

    private TopologyService topologyService;

    public ReadReplicaEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory, MemberId myself )
    {
        this.globalModule = globalModule;
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.globalHealth = globalModule.getGlobalHealthService();
        this.myself = myself;
        LogService logService = globalModule.getLogService();
        this.globaConfig = globalModule.getGlobalConfig();
        this.defaultDatabaseId = new DatabaseId( globaConfig.get( GraphDatabaseSettings.default_database ) );
        logProvider = logService.getInternalLogProvider();
        logProvider.getLog( getClass() ).info( String.format( "Generated new id: %s", myself ) );

        jobScheduler = globalModule.getJobScheduler();
        jobScheduler.setTopLevelGroupName( "ReadReplica " + myself );

        Dependencies globalDependencies = globalModule.getGlobalDependencies();

        panicService = new PanicService( logService.getUserLogProvider() );
        // used in tests
        globalDependencies.satisfyDependencies( panicService );

        LifeSupport globalLife = globalModule.getGlobalLife();

        threadToTransactionBridge = globalDependencies.satisfyDependency( new ThreadToStatementContextBridge() );
        this.accessCapability = new ReadOnly();

        watcherServiceFactory = layout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), layout, logService,
                fileWatcherFileNameFilter() );

        sslPolicyLoader = SslPolicyLoader.create( globaConfig, logProvider );
        globalDependencies.satisfyDependency( sslPolicyLoader );

        PipelineBuilders pipelineBuilders = new PipelineBuilders( this::pipelineWrapperFactory, globaConfig, sslPolicyLoader );
        catchupComponentsProvider = new CatchupComponentsProvider( globalModule, pipelineBuilders );

        editionInvariants( globalModule, globalDependencies, globaConfig, globalLife );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        globalProcedures.register( new ReadReplicaRoleProcedure() );
        globalProcedures.register( new ClusterOverviewProcedure( topologyService, logProvider ) );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        return new ReadReplicaRoutingProcedureInstaller( databaseManager, portRegister, config );
    }

    private void addPanicEventHandlers( PanicService panicService, LifeSupport life, Health allHealths, Server catchupServer,
            Optional<Server> backupServer )
    {
        // order matters
        panicService.addPanicEventHandler( PanicEventHandlers.raiseAvailabilityGuardEventHandler( globalModule.getGlobalAvailabilityGuard() ) );
        panicService.addPanicEventHandler( PanicEventHandlers.dbHealthEventHandler( allHealths ) );
        panicService.addPanicEventHandler( PanicEventHandlers.stopServerEventHandler( catchupServer ) );
        backupServer.ifPresent( server -> panicService.addPanicEventHandler( PanicEventHandlers.stopServerEventHandler( server ) ) );
        panicService.addPanicEventHandler( PanicEventHandlers.shutdownLifeCycle( life ) );
    }

    @Override
    public TransactionHeaderInformationFactory getHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    @Override
    public EditionDatabaseComponents createDatabaseComponents( DatabaseId databaseId )
    {
        return new ReadReplicaDatabaseComponents( globalModule, this, databaseId );
    }

    @Override
    public DatabaseManager<?> createDatabaseManager( GraphDatabaseFacade facade, GlobalModule platform, Log log )
    {
        var databaseManager = new ClusteredMultiDatabaseManager( platform, this, log, facade,
                catchupComponentsProvider::createDatabaseComponents, platform.getFileSystem(), platform.getPageCache(),
                logProvider, globaConfig, globalHealth, globalModule.getGlobalAvailabilityGuard() );
        createDatabaseManagerDependentModules( databaseManager );
        return databaseManager;
    }

    private void createDatabaseManagerDependentModules( ClusteredDatabaseManager databaseManager )
    {
        LifeSupport globalLife = globalModule.getGlobalLife();
        Dependencies dependencies = globalModule.getGlobalDependencies();
        LogService logService = globalModule.getLogService();
        LogProvider internalLogProvider = logService.getInternalLogProvider();
        LogProvider userLogProvider = logService.getUserLogProvider();

        topologyService = createTopologyService( databaseManager, logService );
        globalLife.add( dependencies.satisfyDependency( topologyService ) );

        CatchupHandlerFactory handlerFactory = ignored -> getHandlerFactory( globalModule.getFileSystem(), databaseManager );
        ReadReplicaServerModule serverModule = new ReadReplicaServerModule( databaseManager, catchupComponentsProvider, handlerFactory );

        Executor catchupExecutor = jobScheduler.executor( Group.CATCHUP_CLIENT );
        CommandIndexTracker commandIndexTracker = dependencies.satisfyDependency( new CommandIndexTracker() );
        initialiseStatusDescriptionEndpoint( globalModule, commandIndexTracker );
        TimerService timerService = new TimerService( jobScheduler, logProvider );
        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, globaConfig, logProvider, myself );
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                createUpstreamDatabaseStrategySelector( myself, globaConfig, logProvider, topologyService, defaultStrategy );

        CatchupProcessManager catchupProcessManager =
                new CatchupProcessManager( catchupExecutor, serverModule.catchupComponents(), databaseManager,
                        globalHealth, topologyService, serverModule.catchupClient(), upstreamDatabaseStrategySelector, timerService,
                        commandIndexTracker, internalLogProvider, globalModule.getVersionContextSupplier(),
                        globalModule.getTracers().getPageCursorTracerSupplier(), globaConfig );

        addPanicEventHandlers( panicService, globalModule.getGlobalLife(), globalHealth,
                serverModule.catchupServer(), serverModule.backupServer() );

        globalModule.getGlobalLife().add( new ReadReplicaStartupProcess( catchupExecutor, databaseManager, catchupProcessManager,
                upstreamDatabaseStrategySelector, logProvider, userLogProvider, topologyService, serverModule.catchupComponents() ) );

        globalLife.add( serverModule.catchupServer() ); // must start last and stop first, since it handles external requests
        serverModule.backupServer().ifPresent( globalLife::add );
    }

    @Override
    public void createDatabases( DatabaseManager<?> databaseManager, Config config ) throws DatabaseExistsException
    {
        var initialDatabases = new LinkedHashMap<DatabaseId,OperatorState>();

        initialDatabases.put( new DatabaseId( SYSTEM_DATABASE_NAME ), STOPPED );
        initialDatabases.put( new DatabaseId( config.get( default_database ) ), STOPPED );

        initialDatabases.keySet().forEach( databaseManager::createDatabase );

        setupDatabaseOperators( databaseManager, initialDatabases );
    }

    private void setupDatabaseOperators( DatabaseManager<?> databaseManager, Map<DatabaseId,OperatorState> initialDatabases )
    {
        var reconciler = new ReconcilingDatabaseOperator( databaseManager, initialDatabases );
        var connector = new OperatorConnector( reconciler );

        var localOperator = new LocalOperator( connector );
        var internalOperator = new InternalOperator( connector );
        var systemOperator = new SystemOperator( connector );

        globalModule.getGlobalDependencies().satisfyDependencies( internalOperator ); // for internal components
        globalModule.getGlobalDependencies().satisfyDependencies( localOperator ); // for admin procedures
    }

    private DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
    }

    private CatchupServerHandler getHandlerFactory( FileSystemAbstraction fileSystem, DatabaseManager<?> databaseManager )
    {
        return new MultiDatabaseCatchupServerHandler( databaseManager, logProvider, fileSystem );
    }

    private void initialiseStatusDescriptionEndpoint( GlobalModule globalModule, CommandIndexTracker commandIndexTracker )
    {
        Duration samplingWindow = globaConfig.get( status_throughput_window );
        ThroughputMonitor throughputMonitor = new ThroughputMonitor( logProvider, globalModule.getGlobalClock(),
                globalModule.getJobScheduler(), samplingWindow, commandIndexTracker::getAppliedCommandIndex );
        globalModule.getGlobalLife().add( throughputMonitor );
        globalModule.getGlobalDependencies().satisfyDependency( throughputMonitor );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        SecurityProvider securityProvider;
        if ( globaConfig.get( GraphDatabaseSettings.auth_enabled ) )
        {
            CommercialSecurityModule securityModule = (CommercialSecurityModule) setupSecurityModule( globalModule, this,
                    globalModule.getLogService().getUserLog( ReadReplicaEditionModule.class ), globalProcedures, "commercial-security-module" );
            globalModule.getGlobalLife().add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = CommercialNoAuthSecurityProvider.INSTANCE;
        }
        setSecurityProvider( securityProvider );
    }
    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
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

    private TopologyService createTopologyService( DatabaseManager<?> databaseManager, LogService logService )
    {
        DiscoveryMember discoveryMember = new DefaultDiscoveryMember( myself, databaseManager );
        RemoteMembersResolver hostnameResolver = ResolutionResolverFactory.chooseResolver( globaConfig, logService );
        RetryStrategy retryStrategy = resolveStrategy( globaConfig );
        return discoveryServiceFactory.readReplicaTopologyService( globaConfig, logProvider, jobScheduler, discoveryMember, hostnameResolver,
                retryStrategy, sslPolicyLoader );
    }

    private static RetryStrategy resolveStrategy( Config config )
    {
        long refreshPeriodMillis = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        int pollingFrequencyWithinRefreshWindow = 2;
        int numberOfRetries =
                pollingFrequencyWithinRefreshWindow + 1; // we want to have more retries at the given frequency than there is time in a refresh period
        long delayInMillis = refreshPeriodMillis / pollingFrequencyWithinRefreshWindow;
        return new RetryStrategy( delayInMillis, (long) numberOfRetries );
    }

    /**
     * Returns {@code true} because {@link DatabaseManager}'s lifecycle is managed by {@link ClusteredDatabaseManager} via {@link ReadReplicaStartupProcess}.
     * So {@link DatabaseManager} does not need to be included in the global lifecycle.
     *
     * @return always {@code true}.
     */
    @Override
    public boolean handlesDatabaseManagerLifecycle()
    {
        return true;
    }
}
