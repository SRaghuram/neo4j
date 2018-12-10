/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.discovery.ResolutionResolverFactory;
import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;
import com.neo4j.causalclustering.error_handling.PanicEventHandlers;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import com.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import com.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;
import com.neo4j.server.security.enterprise.CommercialSecurityModule;

import java.io.File;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.common.DefaultDatabaseService;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import com.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import com.neo4j.causalclustering.helper.CompositeSuspendable;
import com.neo4j.causalclustering.identity.MemberId;

import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.udc.UsageData;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

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
    private final Config config;
    private final PlatformModule platformModule;
    private final GlobalTransactionStats transactionStats;

    public ReadReplicaEditionModule( final PlatformModule platformModule, final DiscoveryServiceFactory discoveryServiceFactory, MemberId myself )
    {
        this.platformModule = platformModule;
        LogService logging = platformModule.logService;
        this.config = platformModule.config;
        this.activeDatabaseName = config.get( GraphDatabaseSettings.active_database );
        logProvider = platformModule.logService.getInternalLogProvider();
        LogProvider userLogProvider = platformModule.logService.getUserLogProvider();
        logProvider.getLog( getClass() ).info( String.format( "Generated new id: %s", myself ) );

        platformModule.jobScheduler.setTopLevelGroupName( "ReadReplica " + myself );

        Dependencies dependencies = platformModule.dependencies;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;

        final PanicService panicService = new PanicService( logging.getUserLogProvider() );
        // used in tests
        dependencies.satisfyDependencies( panicService );

        LifeSupport life = platformModule.life;

        threadToTransactionBridge = dependencies.satisfyDependency(
                new ThreadToStatementContextBridge( getGlobalAvailabilityGuard( platformModule.clock, logging, platformModule.config ) ) );
        this.accessCapability = new ReadOnly();

        watcherServiceFactory =
                layout -> createDatabaseFileSystemWatcher( platformModule.fileSystemWatcher.getFileWatcher(), layout, logging, fileWatcherFileNameFilter() );

        RemoteMembersResolver hostnameResolver = ResolutionResolverFactory.chooseResolver( config, platformModule.logService);

        dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );

        configureDiscoveryService( discoveryServiceFactory, dependencies, config, logProvider );

        topologyService = discoveryServiceFactory.readReplicaTopologyService( config, logProvider, platformModule.jobScheduler, myself, hostnameResolver,
                resolveStrategy( config, logProvider ) );

        life.add( dependencies.satisfyDependency( topologyService ) );

        PipelineBuilders pipelineBuilders = new PipelineBuilders( this::pipelineWrapperFactory, logProvider, config, dependencies );

        Supplier<DatabaseManager> databaseManagerSupplier = () -> platformModule.dependencies.resolveDependency( DatabaseManager.class );
        Supplier<DatabaseHealth> databaseHealthSupplier =
                () -> databaseManagerSupplier.get().getDatabaseContext( DEFAULT_DATABASE_NAME )
                        .map( DatabaseContext::getDependencies)
                        .map( resolver -> resolver.resolveDependency( DatabaseHealth.class ) )
                        .orElseThrow( () -> new IllegalStateException( "Default database not found." ) );
        this.databaseService = createDatabasesService( databaseHealthSupplier, fileSystem, globalAvailabilityGuard, platformModule,
                databaseManagerSupplier, logProvider, config );

        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, config, logProvider, myself );

        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                createUpstreamDatabaseStrategySelector( myself, config, logProvider, topologyService, defaultStrategy );

        CatchupHandlerFactory handlerFactory = ignored -> getHandlerFactory( platformModule, fileSystem );
        ReadReplicaServerModule serverModule =
                new ReadReplicaServerModule( databaseService, pipelineBuilders, handlerFactory, platformModule, activeDatabaseName );

        CommandIndexTracker commandIndexTracker = platformModule.dependencies.satisfyDependency( new CommandIndexTracker() );

        CompositeSuspendable servicesToStopOnStoreCopy = new CompositeSuspendable();
        Executor catchupExecutor = platformModule.jobScheduler.executor( Group.CATCHUP_CLIENT );

        TimerService timerService = new TimerService( platformModule.jobScheduler, logProvider );

        CatchupProcessManager catchupProcessManager =
                new CatchupProcessManager( catchupExecutor, serverModule.catchupComponents(), databaseService, servicesToStopOnStoreCopy,
                        databaseHealthSupplier, topologyService, serverModule.catchupClient(), upstreamDatabaseStrategySelector, timerService,
                        commandIndexTracker, platformModule.logService.getInternalLogProvider(), platformModule.versionContextSupplier,
                        platformModule.tracers.getPageCursorTracerSupplier(), platformModule.config );

        life.add( new ReadReplicaStartupProcess( catchupExecutor, databaseService, catchupProcessManager, upstreamDatabaseStrategySelector,
                logProvider, userLogProvider, topologyService, serverModule.catchupComponents() ) );

        servicesToStopOnStoreCopy.add( serverModule.catchupServer() );
        serverModule.backupServer().ifPresent( servicesToStopOnStoreCopy::add );

        life.add( serverModule.catchupServer() ); // must start last and stop first, since it handles external requests
        serverModule.backupServer().ifPresent( life::add );

        editionInvariants( platformModule, dependencies, config, life );

        addPanicEventHandlers( panicService, life, databaseHealthSupplier, serverModule.catchupServer(), serverModule.backupServer() );

        this.transactionStats = new GlobalTransactionStats();
        initGlobalGuard( platformModule.clock, platformModule.logService );
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        procedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        procedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        procedures.register( new ReadReplicaRoleProcedure() );
        procedures.register( new ClusterOverviewProcedure( topologyService, logProvider ) );
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
        return new ReadReplicaDatabaseContext( platformModule, this, databaseName );
    }

    private DefaultDatabaseService<ReadReplicaLocalDatabase> createDatabasesService( Supplier<DatabaseHealth> databaseHealthSupplier,
            FileSystemAbstraction fileSystem, AvailabilityGuard availabilityGuard, PlatformModule platformModule,
            Supplier<DatabaseManager> databaseManagerSupplier, LogProvider logProvider, Config config )
    {
        return new DefaultDatabaseService<>( ReadReplicaLocalDatabase::new, databaseManagerSupplier, platformModule.storeLayout,
                availabilityGuard, databaseHealthSupplier, fileSystem, platformModule.pageCache, logProvider, config );
    }

    @Override
    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, PlatformModule platform, AbstractEditionModule edition,
            Procedures procedures, Logger msgLog )
    {
        return new MultiDatabaseManager( platform, edition, procedures, msgLog, graphDatabaseFacade );
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

    protected CatchupServerHandler getHandlerFactory( PlatformModule platformModule, FileSystemAbstraction fileSystem )
    {
        Supplier<DatabaseManager> databaseManagerSupplier = platformModule.dependencies.provideDependency( DatabaseManager.class );
        return new MultiDatabaseCatchupServerHandler( databaseManagerSupplier, logProvider, fileSystem );
    }

    @Override
    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( String databaseName, Clock clock, LogService logService, Config config )
    {
        return ((CompositeDatabaseAvailabilityGuard) getGlobalAvailabilityGuard( clock, logService, config )).createDatabaseAvailabilityGuard( databaseName );
    }

    @Override
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        SecurityProvider securityProvider;
        if ( platformModule.config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            CommercialSecurityModule securityModule = (CommercialSecurityModule) setupSecurityModule( platformModule, this,
                    platformModule.logService.getUserLog( ReadReplicaEditionModule.class ), procedures, "commercial-security-module" );
            platformModule.life.add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = EnterpriseNoAuthSecurityProvider.INSTANCE;
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
