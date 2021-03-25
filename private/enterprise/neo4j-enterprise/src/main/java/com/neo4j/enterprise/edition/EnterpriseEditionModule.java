/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupServerBuilder;
import com.neo4j.causalclustering.catchup.CatchupServerProvider;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.ConfigurableTransactionStreamingStrategy;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.common.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.core.state.DiscoveryModule;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.error_handling.DbmsPanicker;
import com.neo4j.causalclustering.error_handling.DefaultPanicService;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.EnterpriseEditionSettings;
import com.neo4j.configuration.FabricEnterpriseConfig;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.EnterpriseSystemGraphDbmsModel;
import com.neo4j.dbms.StandaloneDbmsReconcilerModule;
import com.neo4j.dbms.TopologyPublisher;
import com.neo4j.dbms.database.EnterpriseMultiDatabaseManager;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.dbms.database.StandaloneEnterpriseDatabaseContext;
import com.neo4j.fabric.auth.FabricAuthManagerWrapper;
import com.neo4j.fabric.bootstrap.EnterpriseFabricServicesBootstrap;
import com.neo4j.fabric.localdb.FabricSystemGraphComponent;
import com.neo4j.fabric.routing.FabricRoutingProcedureInstaller;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import com.neo4j.procedure.enterprise.builtin.SettingsWhitelist;
import com.neo4j.server.enterprise.EnterpriseNeoWebServer;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseDefaultDatabaseResolver;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.cypher.internal.javacompat.EnterpriseCypherEngineProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.identity.StandaloneIdentityModule;
import org.neo4j.exceptions.KernelException;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenHolders;

import static com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler.backupServerHandler;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.function.Predicates.any;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.DEFAULT_FILENAME_PREDICATE;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

public class EnterpriseEditionModule extends CommunityEditionModule implements AbstractEnterpriseEditionModule
{
    private final ReconciledTransactionTracker reconciledTxTracker;
    private final EnterpriseFabricServicesBootstrap fabricServicesBootstrap;
    private final Dependencies dependencies;
    private final PanicService panicService;
    private final StandaloneIdentityModule identityModule;
    private final SecurityLog securityLog;
    private final InstalledProtocolHandler installedProtocolHandler;

    private DatabaseStartAborter databaseStartAborter;
    private StandaloneDbmsReconcilerModule reconcilerModule;

    private TopologyService topologyService;
    private CatchupClientFactory catchupClientFactory;

    public EnterpriseEditionModule( GlobalModule globalModule )
    {
        this( globalModule, globalModule.getGlobalDependencies() );
    }

    protected EnterpriseEditionModule( GlobalModule globalModule, Dependencies dependencies  )
    {
        super( globalModule );
        this.dependencies = dependencies;

        satisfyEnterpriseOnlyDependencies( globalModule );
        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig(), globalModule.getGlobalClock() );
        reconciledTxTracker = new DefaultReconciledTransactionTracker( globalModule.getLogService() );
        fabricServicesBootstrap = new EnterpriseFabricServicesBootstrap.Single( globalModule.getGlobalLife(), dependencies, globalModule.getLogService() );
        SettingsWhitelist settingsWhiteList = new SettingsWhitelist( globalModule.getGlobalConfig() );
        dependencies.satisfyDependency( settingsWhiteList );
        panicService = new DefaultPanicService( globalModule.getJobScheduler(), globalModule.getLogService(),
                DbmsPanicker.buildFor( globalModule.getGlobalConfig(), globalModule.getLogService() ) );
        identityModule = StandaloneIdentityModule.fromGlobalModule( globalModule );
        dependencies.satisfyDependency( identityModule );
        securityLog = new SecurityLog( globalModule.getGlobalConfig(), globalModule.getFileSystem() );
        globalModule.getGlobalLife().add( securityLog );
        installedProtocolHandler = new InstalledProtocolHandler();
    }

    @Override
    public QueryEngineProvider getQueryEngineProvider()
    {
        return new EnterpriseCypherEngineProvider();
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager ) throws KernelException
    {
        new EnterpriseProceduresInstaller( globalProcedures, databaseManager, reconcilerModule, globalModule, identityModule,
                installedProtocolHandler, catchupClientFactory, topologyService ).register();
        fabricServicesBootstrap.registerProcedures( globalProcedures );
    }

    @Override
    protected Predicate<String> fileWatcherFileNameFilter()
    {
        return any( DEFAULT_FILENAME_PREDICATE,
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }

    @Override
    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new EnterpriseConstraintSemantics();
    }

    @Override
    protected Function<NamedDatabaseId,TokenHolders> createTokenHolderProvider( GlobalModule platform )
    {
        Config globalConfig = platform.getGlobalConfig();
        return databaseId -> {
            DatabaseManager<?> databaseManager = platform.getGlobalDependencies().resolveDependency( DatabaseManager.class );
            Supplier<Kernel> kernelSupplier = () ->
            {
                DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseId )
                        .orElseThrow( () -> new IllegalStateException( format( "Database for %s not found.", databaseId ) ) );
                return databaseContext.dependencies().resolveDependency( Kernel.class );
            };
            return new TokenHolders(
                    new DelegatingTokenHolder( createPropertyKeyCreator( globalConfig, databaseId, kernelSupplier ), TYPE_PROPERTY_KEY ),
                    new DelegatingTokenHolder( createLabelIdCreator( globalConfig, databaseId, kernelSupplier ), TYPE_LABEL ),
                    new DelegatingTokenHolder( createRelationshipTypeCreator( globalConfig, databaseId, kernelSupplier ), TYPE_RELATIONSHIP_TYPE ) );
        };
    }

    @Override
    public DatabaseManager<? extends StandaloneDatabaseContext> createDatabaseManager( GlobalModule globalModule )
    {
        var databaseManager = new EnterpriseMultiDatabaseManager( globalModule, this, this::getTopologyPublisher );

        globalModule.getGlobalLife().add( databaseManager );
        globalModule.getGlobalDependencies().satisfyDependency( databaseManager );

        Supplier<GraphDatabaseService> systemDbSupplier = () -> databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID )
                .orElseThrow()
                .databaseFacade();
        var dbmsModel = new EnterpriseSystemGraphDbmsModel( systemDbSupplier );
        reconcilerModule = new StandaloneDbmsReconcilerModule( globalModule, databaseManager, reconciledTxTracker, dbmsModel );
        databaseStateService = reconcilerModule.databaseStateService();
        databaseStartAborter = new DatabaseStartAborter( globalModule.getGlobalAvailabilityGuard(), dbmsModel, globalModule.getGlobalClock(),
                Duration.ofSeconds( 5 ) );
        globalModule.getGlobalLife().add( reconcilerModule );
        globalModule.getGlobalDependencies().satisfyDependency( reconciledTxTracker );

        createDatabaseManagerDependentModules( databaseManager, reconcilerModule );

        return databaseManager;
    }

    private void createDatabaseManagerDependentModules( MultiDatabaseManager<StandaloneEnterpriseDatabaseContext> databaseManager,
            StandaloneDbmsReconcilerModule reconcilerModule )
    {
        initDiscoveryAndCatchupIfNeeded( databaseManager, reconcilerModule );
        initBackupIfNeeded( globalModule, globalModule.getGlobalConfig(), databaseManager );
    }

    @Override
    public void registerSystemGraphComponents( SystemGraphComponents systemGraphComponents, GlobalModule globalModule )
    {
        var fabricDatabaseManager = dependencies.resolveDependency( FabricDatabaseManager.class );
        var systemGraphComponent = new FabricSystemGraphComponent( globalModule.getGlobalConfig(), fabricDatabaseManager );
        systemGraphComponents.register( systemGraphComponent );
        registerSecurityComponents( systemGraphComponents, globalModule, securityLog );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        setSecurityProvider( makeEnterpriseSecurityModule( globalModule, defaultDatabaseResolver, securityLog ) );
    }

    @Override
    public void createDefaultDatabaseResolver( GlobalModule globalModule )
    {
        EnterpriseDefaultDatabaseResolver defaultDatabaseResolver = makeDefaultDatabaseResolver( globalModule );
        globalModule.getTransactionEventListeners().registerTransactionEventListener( SYSTEM_DATABASE_NAME, defaultDatabaseResolver );
        setDefaultDatabaseResolver( defaultDatabaseResolver );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        var fabricConfig = dependencies.resolveDependency( FabricEnterpriseConfig.class );
        var fabricDatabaseManager = dependencies.resolveDependency( FabricDatabaseManager.class );
        return new FabricRoutingProcedureInstaller( databaseManager, portRegister, config, fabricDatabaseManager, fabricConfig, logProvider );
    }

    @Override
    public AuthManager getBoltAuthManager( DependencyResolver dependencyResolver )
    {
        AuthManager authManager = super.getBoltAuthManager( dependencyResolver );
        var fabricDatabaseManager = dependencyResolver.resolveDependency( FabricDatabaseManager.class );

        if ( !fabricDatabaseManager.isFabricDatabasePresent() )
        {
            return authManager;
        }

        if ( !(authManager instanceof EnterpriseAuthManager) )
        {
            throw new IllegalStateException( "Unexpected type of Auth manager: " + authManager.getClass() );
        }

        return new FabricAuthManagerWrapper( (EnterpriseAuthManager) authManager );
    }

    @Override
    public DatabaseStartupController getDatabaseStartupController()
    {
        return databaseStartAborter;
    }

    @Override
    public Lifecycle createWebServer( DatabaseManagementService managementService, Dependencies globalDependencies, Config config,
            LogProvider userLogProvider, DbmsInfo dbmsInfo )
    {
        return new EnterpriseNeoWebServer( managementService, globalDependencies, config, userLogProvider, dbmsInfo );
    }

    @Override
    public void bootstrapFabricServices()
    {
        fabricServicesBootstrap.bootstrapServices();
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var kernelDatabaseManagementService = createBoltKernelDatabaseManagementServiceProvider(dependencies, managementService, monitors, clock, logService);
        return fabricServicesBootstrap.createBoltDatabaseManagementServiceProvider( kernelDatabaseManagementService, managementService, monitors, clock );
    }

    private void initBackupIfNeeded( GlobalModule globalModule, Config config, MultiDatabaseManager<StandaloneEnterpriseDatabaseContext> databaseManager )
    {
        FileSystemAbstraction fs = globalModule.getFileSystem();
        JobScheduler jobScheduler = globalModule.getJobScheduler();
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();

        LogProvider internalLogProvider = globalModule.getLogService().getInternalLogProvider();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, internalLogProvider );
        PipelineBuilders pipelineBuilders = new PipelineBuilders( sslPolicyLoader );

        int maxChunkSize = config.get( CausalClusteringSettings.store_copy_chunk_size );
        var backupStrategyProvider = ConfigurableTransactionStreamingStrategy.create( config );
        TransactionBackupServiceProvider backupServiceProvider = new TransactionBackupServiceProvider(
                internalLogProvider, supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration(),
                supportedProtocolCreator.createSupportedModifierProtocols(),
                pipelineBuilders.backupServer(),
                backupServerHandler( databaseManager, databaseStateService, fs, maxChunkSize, internalLogProvider,
                        globalModule.getGlobalDependencies(),
                        backupStrategyProvider ),
                installedProtocolHandler,
                jobScheduler,
                portRegister,
                globalModule.getCentralBufferMangerHolder().getNettyBufferAllocator() );

        Optional<Server> backupServer = backupServiceProvider.resolveIfBackupEnabled( config );

        backupServer.ifPresent( globalModule.getGlobalLife()::add );
    }

    private void initDiscoveryAndCatchupIfNeeded( MultiDatabaseManager<StandaloneEnterpriseDatabaseContext> databaseManager,
            StandaloneDbmsReconcilerModule reconcilerModule )
    {
        if ( clusteringEnabled( globalModule.getGlobalConfig() ) )
        {
            createTopologyService( reconcilerModule );
            createCatchupService( databaseManager );
        }
    }

    private void createTopologyService( StandaloneDbmsReconcilerModule reconcilerModule )
    {
        var discoveryModule =
                new DiscoveryModule( new AkkaDiscoveryServiceFactory(), globalModule, sslPolicyLoader, databaseStateService, panicService.panicker() );
        topologyService = discoveryModule.standaloneTopologyService( identityModule );

        reconcilerModule.registerDatabaseStateChangedListener( topologyService );
    }

    private void createCatchupService( MultiDatabaseManager<StandaloneEnterpriseDatabaseContext> databaseManager )
    {
        var config = globalModule.getGlobalConfig();
        var logProvider = globalModule.getLogService().getInternalLogProvider();
        var supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );

        int maxChunkSize = config.get( CausalClusteringSettings.store_copy_chunk_size );
        var catchupServerHandler = MultiDatabaseCatchupServerHandler.catchupServerHandler( databaseManager, databaseStateService, globalModule.getFileSystem(),
                maxChunkSize, logProvider, globalModule.getGlobalDependencies() );

        PipelineBuilders pipelineBuilders = new PipelineBuilders( sslPolicyLoader );

        var catchupServer = CatchupServerBuilder.builder()
                .catchupServerHandler( catchupServerHandler )
                .catchupProtocols( supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration() )
                .modifierProtocols( supportedProtocolCreator.createSupportedModifierProtocols() )
                .pipelineBuilder( pipelineBuilders.server() )
                .installedProtocolsHandler( installedProtocolHandler )
                .listenAddress( config.get( CausalClusteringSettings.transaction_listen_address ) )
                .scheduler( globalModule.getJobScheduler() )
                .config( config )
                .bootstrapConfig( BootstrapConfiguration.serverConfig( config ) )
                .portRegister( globalModule.getConnectorPortRegister() )
                .userLogProvider( globalModule.getLogService().getUserLogProvider() )
                .debugLogProvider( logProvider )
                .serverName( "catchup-server" )
                .handshakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .build();

        catchupClientFactory = new CatchupComponentsProvider( globalModule, pipelineBuilders ).catchupClientFactory();

        globalModule.getGlobalLife().add( catchupServer );
        // used by ReadReplicaHierarchicalCatchupIT
        globalModule.getGlobalDependencies().satisfyDependencies( (CatchupServerProvider) () -> catchupServer );
    }

    private TopologyPublisher getTopologyPublisher( NamedDatabaseId namedDatabaseId )
    {
        if ( topologyService == null )
        {
            return TopologyPublisher.NOOP;
        }
        return TopologyPublisher.from( namedDatabaseId, topologyService::onDatabaseStart, topologyService::onDatabaseStop );
    }

    public static boolean clusteringEnabled( Config config )
    {
        return config.get( EnterpriseEditionSettings.enable_clustering_in_standalone );
    }
}
