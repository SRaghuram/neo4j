/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.io.File;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.catchup.CheckPointerService;
import org.neo4j.causalclustering.catchup.RegularCatchupServerHandler;
import org.neo4j.causalclustering.common.DefaultDatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.common.PipelineBuilders;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import org.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.causalclustering.discovery.RetryStrategy;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.helper.CompositeSuspendable;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.udc.UsageData;

import static org.neo4j.causalclustering.discovery.ResolutionResolverFactory.chooseResolver;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Enterprise Read Replica edition.
 */
public class EnterpriseReadReplicaEditionModule extends AbstractEditionModule
{
    private final TopologyService topologyService;
    protected final LogProvider logProvider;
    protected final DefaultDatabaseService<ReadReplicaLocalDatabase> databaseService;
    private final String activeDatabaseName;
    private final Config config;
    private final PlatformModule platformModule;

    public EnterpriseReadReplicaEditionModule( final PlatformModule platformModule, final DiscoveryServiceFactory discoveryServiceFactory, MemberId myself )
    {
        this.platformModule = platformModule;
        LogService logging = platformModule.logService;
        this.config = platformModule.config;
        this.activeDatabaseName = config.get( GraphDatabaseSettings.active_database );
        logProvider = platformModule.logService.getInternalLogProvider();
        LogProvider userLogProvider = platformModule.logService.getUserLogProvider();
        logProvider.getLog( getClass() ).info( String.format( "Generated new id: %s", myself ) );

        ioLimiter = new ConfigurableIOLimiter( platformModule.config );
        platformModule.jobScheduler.setTopLevelGroupName( "ReadReplica " + myself );

        Dependencies dependencies = platformModule.dependencies;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        PageCache pageCache = platformModule.pageCache;

        LifeSupport life = platformModule.life;

        threadToTransactionBridge = dependencies.satisfyDependency(
                new ThreadToStatementContextBridge( getGlobalAvailabilityGuard( platformModule.clock, logging, platformModule.config ) ) );
        this.accessCapability = new ReadOnly();

        watcherServiceFactory =
                layout -> createDatabaseFileSystemWatcher( platformModule.fileSystemWatcher.getFileWatcher(), layout, logging, fileWatcherFileNameFilter() );

        File contextDirectory = platformModule.storeLayout.storeDirectory();
        life.add( dependencies.satisfyDependency( new KernelData( fileSystem, pageCache, contextDirectory, config ) ) );

        headerInformationFactory = TransactionHeaderInformationFactory.DEFAULT;
        schemaWriteGuard = () ->
        {
        };

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = new EnterpriseConstraintSemantics();

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );

        connectionTracker = dependencies.satisfyDependency( createConnectionTracker() );

        RemoteMembersResolver hostnameResolver = chooseResolver( config, platformModule.logService);

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
                        platformModule.tracers.pageCursorTracerSupplier, platformModule.config );

        life.add( new ReadReplicaStartupProcess( catchupExecutor, databaseService, catchupProcessManager, upstreamDatabaseStrategySelector,
                logProvider, userLogProvider, topologyService, serverModule.catchupComponents() ) );

        servicesToStopOnStoreCopy.add( serverModule.catchupServer() );
        serverModule.backupServer().ifPresent( servicesToStopOnStoreCopy::add );

        life.add( serverModule.catchupServer() ); // must start last and stop first, since it handles external requests
        serverModule.backupServer().ifPresent( life::add );
    }

    //TODO: Create Shared EditionModule abstract class
    private DefaultDatabaseService<ReadReplicaLocalDatabase> createDatabasesService( Supplier<DatabaseHealth> databaseHealthSupplier,
            FileSystemAbstraction fileSystem, AvailabilityGuard availabilityGuard, PlatformModule platformModule,
            Supplier<DatabaseManager> databaseManagerSupplier, LogProvider logProvider, Config config )
    {
        return new DefaultDatabaseService<>( ReadReplicaLocalDatabase::new, databaseManagerSupplier, platformModule.storeLayout,
                availabilityGuard, databaseHealthSupplier, fileSystem, platformModule.pageCache, platformModule.jobScheduler, logProvider, config );
    }

    //TODO: Put method on common interface to prevent visibility changes, need to override in commercial edition modules.
    protected CatchupServerHandler getHandlerFactory( PlatformModule platformModule, FileSystemAbstraction fileSystem )
    {
        //TODO: Remove all these suppliers!
        Supplier<LocalDatabase> localDatabase = () -> databaseService.get( activeDatabaseName ).orElseThrow( IllegalStateException::new );

        // TODO: Error handling
        CheckPointerService checkPointerService = new CheckPointerService(
                () -> localDatabase.get().dependencies().resolveDependency( CheckPointer.class ),
                platformModule.jobScheduler, Group.CHECKPOINT );

        return new RegularCatchupServerHandler( platformModule.monitors, logProvider, () -> localDatabase.get().storeId(),
                () -> localDatabase.get().database(), databaseService::areAvailable, fileSystem, null, checkPointerService );
    }

    @Override
    public EditionDatabaseContext createDatabaseContext( String databaseName )
    {
        return new ReadReplicaDatabaseContext( platformModule, this, databaseName );
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

    protected void configureDiscoveryService( DiscoveryServiceFactory discoveryServiceFactory, Dependencies dependencies, Config config,
            LogProvider logProvider )
    {
    }

    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new VoidPipelineWrapperFactory();
    }

    static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any( fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        procedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        procedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        procedures.register( new ReadReplicaRoleProcedure() );
        procedures.register( new ClusterOverviewProcedure( topologyService, logProvider ) );
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    @Override
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.createEnterpriseSecurityModule( this, platformModule, procedures );
    }

    @Override
    public void createDatabases( DatabaseManager databaseManager, Config config )
    {
        createDatabase( databaseManager, activeDatabaseName );
    }

    protected void createDatabase( DatabaseManager databaseManager, String databaseName )
    {
        databaseService.registerDatabase( databaseName );
        databaseManager.createDatabase( databaseName );
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
