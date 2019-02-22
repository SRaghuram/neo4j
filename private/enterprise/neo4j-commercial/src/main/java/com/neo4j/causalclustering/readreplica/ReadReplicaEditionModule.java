/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.common.DefaultDatabaseService;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.ResolutionResolverFactory;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import com.neo4j.causalclustering.error_handling.PanicEventHandlers;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.helper.CompositeSuspendable;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import com.neo4j.kernel.enterprise.api.security.provider.CommercialNoAuthSecurityProvider;
import com.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;
import com.neo4j.server.security.enterprise.CommercialSecurityModule;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Commercial Read Replica edition.
 */
public class ReadReplicaEditionModule extends ClusteringEditionModule
{
    private final TopologyService topologyService;
    protected final LogProvider logProvider;
    protected final DefaultDatabaseService<ReadReplicaLocalDatabase> databaseService;
    private final String activeDatabaseName;
    private final Config globaConfig;
    private final GlobalModule globalModule;
    private final GlobalTransactionStats transactionStats;

    public ReadReplicaEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory, MemberId myself )
    {
        this.globalModule = globalModule;
        LogService logService = globalModule.getLogService();
        this.globaConfig = globalModule.getGlobalConfig();
        this.activeDatabaseName = globaConfig.get( GraphDatabaseSettings.active_database );
        logProvider = logService.getInternalLogProvider();
        LogProvider userLogProvider = logService.getUserLogProvider();
        logProvider.getLog( getClass() ).info( String.format( "Generated new id: %s", myself ) );

        JobScheduler jobScheduler = globalModule.getJobScheduler();
        jobScheduler.setTopLevelGroupName( "ReadReplica " + myself );

        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        FileSystemAbstraction fileSystem = globalModule.getFileSystem();

        final PanicService panicService = new PanicService( logService.getUserLogProvider() );
        // used in tests
        globalDependencies.satisfyDependencies( panicService );

        LifeSupport globalLife = globalModule.getGlobalLife();

        SystemNanoClock globalClock = globalModule.getGlobalClock();
        threadToTransactionBridge = globalDependencies.satisfyDependency(
                new ThreadToStatementContextBridge( getGlobalAvailabilityGuard( globalClock, logService, globaConfig ) ) );
        this.accessCapability = new ReadOnly();

        watcherServiceFactory = layout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), layout, logService,
                fileWatcherFileNameFilter() );

        RemoteMembersResolver hostnameResolver = ResolutionResolverFactory.chooseResolver( globaConfig, logService);

        globalDependencies.satisfyDependency( SslPolicyLoader.create( globaConfig, logProvider ) );

        configureDiscoveryService( discoveryServiceFactory, globalDependencies, globaConfig, logProvider );

        topologyService = discoveryServiceFactory.readReplicaTopologyService( globaConfig, logProvider, jobScheduler, myself, hostnameResolver,
                resolveStrategy( globaConfig, logProvider ) );

        globalLife.add( globalDependencies.satisfyDependency( topologyService ) );

        PipelineBuilders pipelineBuilders = new PipelineBuilders( this::pipelineWrapperFactory, logProvider, globaConfig, globalDependencies );

        Supplier<DatabaseManager> databaseManagerSupplier = () -> globalDependencies.resolveDependency( DatabaseManager.class );
        Supplier<DatabaseHealth> databaseHealthSupplier =
                () -> databaseManagerSupplier.get().getDatabaseContext( DEFAULT_DATABASE_NAME )
                        .map( DatabaseContext::getDependencies)
                        .map( resolver -> resolver.resolveDependency( DatabaseHealth.class ) )
                        .orElseThrow( () -> new IllegalStateException( "Default database not found." ) );
        this.databaseService = createDatabasesService( databaseHealthSupplier, fileSystem, globalAvailabilityGuard, globalModule,
                databaseManagerSupplier, logProvider, globaConfig );

        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, globaConfig, logProvider, myself );

        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                createUpstreamDatabaseStrategySelector( myself, globaConfig, logProvider, topologyService, defaultStrategy );

        CatchupHandlerFactory handlerFactory = ignored -> getHandlerFactory( globalModule, fileSystem );
        ReadReplicaServerModule serverModule =
                new ReadReplicaServerModule( databaseService, pipelineBuilders, handlerFactory, globalModule, activeDatabaseName );

        CommandIndexTracker commandIndexTracker = globalDependencies.satisfyDependency( new CommandIndexTracker() );

        CompositeSuspendable servicesToStopOnStoreCopy = new CompositeSuspendable();
        Executor catchupExecutor = jobScheduler.executor( Group.CATCHUP_CLIENT );

        TimerService timerService = new TimerService( jobScheduler, logProvider );

        CatchupProcessManager catchupProcessManager =
                new CatchupProcessManager( catchupExecutor, serverModule.catchupComponents(), databaseService, servicesToStopOnStoreCopy,
                        databaseHealthSupplier, topologyService, serverModule.catchupClient(), upstreamDatabaseStrategySelector, timerService,
                        commandIndexTracker, logService.getInternalLogProvider(), globalModule.getVersionContextSupplier(),
                        globalModule.getTracers().getPageCursorTracerSupplier(), globaConfig );

        globalLife.add( new ReadReplicaStartupProcess( catchupExecutor, databaseService, catchupProcessManager, upstreamDatabaseStrategySelector,
                logProvider, userLogProvider, topologyService, serverModule.catchupComponents() ) );

        servicesToStopOnStoreCopy.add( serverModule.catchupServer() );
        serverModule.backupServer().ifPresent( servicesToStopOnStoreCopy::add );

        globalLife.add( serverModule.catchupServer() ); // must start last and stop first, since it handles external requests
        serverModule.backupServer().ifPresent( globalLife::add );

        editionInvariants( globalModule, globalDependencies, globaConfig, globalLife );

        addPanicEventHandlers( panicService, globalLife, databaseHealthSupplier, serverModule.catchupServer(), serverModule.backupServer() );

        this.transactionStats = new GlobalTransactionStats();
        initGlobalGuard( globalClock, logService );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        ReadReplicaRoutingProcedureInstaller routingProcedureInstaller = new ReadReplicaRoutingProcedureInstaller( portRegister, globaConfig );
        routingProcedureInstaller.install( globalProcedures );

        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        globalProcedures.register( new ReadReplicaRoleProcedure() );
        globalProcedures.register( new ClusterOverviewProcedure( topologyService, logProvider ) );
    }

    private void addPanicEventHandlers( PanicService panicService, LifeSupport life, Supplier<DatabaseHealth> databaseHealthSupplier, Server catchupServer,
            Optional<Server> backupServer )
    {
        // order matters
        panicService.addPanicEventHandler( PanicEventHandlers.raiseAvailabilityGuardEventHandler( globalAvailabilityGuard ) );
        panicService.addPanicEventHandler( PanicEventHandlers.dbHealthEventHandler( databaseHealthSupplier ) );
        panicService.addPanicEventHandler( PanicEventHandlers.disableServerEventHandler( catchupServer ) );
        backupServer.ifPresent( server -> panicService.addPanicEventHandler( PanicEventHandlers.disableServerEventHandler( server ) ) );
        panicService.addPanicEventHandler( PanicEventHandlers.shutdownLifeCycle( life ) );
    }

    @Override
    public TransactionHeaderInformationFactory getHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    @Override
    protected SchemaWriteGuard createSchemaWriteGuard()
    {
        return () ->
        {
        };
    }

    private void configureDiscoveryService( DiscoveryServiceFactory discoveryServiceFactory, Dependencies dependencies,
                                              Config config, LogProvider logProvider )
    {
        SslPolicyLoader sslPolicyFactory = dependencies.resolveDependency( SslPolicyLoader.class );
        SslPolicy clusterSslPolicy = sslPolicyFactory.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );

        if ( discoveryServiceFactory instanceof SslDiscoveryServiceFactory )
        {
            ((SslDiscoveryServiceFactory) discoveryServiceFactory).setSslPolicy( clusterSslPolicy );
        }
    }

    @Override
    public EditionDatabaseContext createDatabaseContext( String databaseName )
    {
        return new ReadReplicaDatabaseContext( globalModule, this, databaseName );
    }

    private DefaultDatabaseService<ReadReplicaLocalDatabase> createDatabasesService( Supplier<DatabaseHealth> databaseHealthSupplier,
            FileSystemAbstraction fileSystem, AvailabilityGuard availabilityGuard, GlobalModule globalModule,
            Supplier<DatabaseManager> databaseManagerSupplier, LogProvider logProvider, Config config )
    {
        return new DefaultDatabaseService<>( ReadReplicaLocalDatabase::new, databaseManagerSupplier, globalModule.getStoreLayout(),
                availabilityGuard, databaseHealthSupplier, fileSystem, globalModule.getPageCache(), logProvider, config );
    }

    @Override
    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, GlobalModule platform, AbstractEditionModule edition,
            GlobalProcedures globalProcedures, Logger msgLog )
    {
        return new MultiDatabaseManager( platform, edition, globalProcedures, msgLog, graphDatabaseFacade );
    }

    @Override
    public void createDatabases( DatabaseManager databaseManager, Config config )
    {
        createCommercialEditionDatabases( databaseManager, config );
    }

    private void createCommercialEditionDatabases( DatabaseManager databaseManager, Config config )
    {
        createDatabase( databaseManager, GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        createConfiguredDatabases( databaseManager, config );
    }

    private void createConfiguredDatabases( DatabaseManager databaseManager, Config config )
    {
        createDatabase( databaseManager, config.get( GraphDatabaseSettings.active_database ) );
    }

    protected void createDatabase( DatabaseManager databaseManager, String databaseName )
    {
        databaseService.registerDatabase( databaseName );
        databaseManager.createDatabase( databaseName );
    }

    private DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
    }

    @Override
    public DatabaseTransactionStats createTransactionMonitor()
    {
        return transactionStats.createDatabaseTransactionMonitor();
    }

    @Override
    public TransactionCounters globalTransactionCounter()
    {
        return transactionStats;
    }

    @Override
    public AvailabilityGuard getGlobalAvailabilityGuard( Clock clock, LogService logService, Config config )
    {
        initGlobalGuard( clock, logService );
        return globalAvailabilityGuard;
    }

    protected CatchupServerHandler getHandlerFactory( GlobalModule globalModule, FileSystemAbstraction fileSystem )
    {
        Supplier<DatabaseManager> databaseManagerSupplier = globalModule.getGlobalDependencies().provideDependency( DatabaseManager.class );
        return new MultiDatabaseCatchupServerHandler( databaseManagerSupplier, logProvider, fileSystem );
    }

    @Override
    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( String databaseName, Clock clock, LogService logService, Config config )
    {
        return ((CompositeDatabaseAvailabilityGuard) getGlobalAvailabilityGuard( clock, logService, config )).createDatabaseAvailabilityGuard( databaseName );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule, GlobalProcedures globalProcedures )
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

    private void initGlobalGuard( Clock clock, LogService logService )
    {
        if ( globalAvailabilityGuard == null )
        {
            globalAvailabilityGuard = new CompositeDatabaseAvailabilityGuard( clock, logService );
        }
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

    private static RetryStrategy resolveStrategy( Config config, LogProvider logProvider )
    {
        long refreshPeriodMillis = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        int pollingFrequencyWithinRefreshWindow = 2;
        int numberOfRetries =
                pollingFrequencyWithinRefreshWindow + 1; // we want to have more retries at the given frequency than there is time in a refresh period
        long delayInMillis = refreshPeriodMillis / pollingFrequencyWithinRefreshWindow;
        return new RetryStrategy( delayInMillis, (long) numberOfRetries );
    }
}
