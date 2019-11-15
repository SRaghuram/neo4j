/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition;

import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.common.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.EnterpriseSystemGraphDbmsModel;
import com.neo4j.dbms.StandaloneDbmsReconcilerModule;
import com.neo4j.dbms.database.EnterpriseMultiDatabaseManager;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.fabric.auth.FabricAuthManagerWrapper;
import com.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;
import com.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.localdb.FabricSystemGraphInitializer;
import com.neo4j.fabric.routing.FabricRoutingProcedureInstaller;
import com.neo4j.fabric.transaction.TransactionManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import com.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInProcedures;
import com.neo4j.procedure.enterprise.builtin.SettingsWhitelist;
import com.neo4j.server.security.enterprise.EnterpriseSecurityModule;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.cypher.internal.javacompat.EnterpriseCypherEngineProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.dbms.procedures.StandaloneDatabaseStateProcedure;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenHolders;

import static java.lang.String.format;
import static org.neo4j.function.Predicates.any;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

public class EnterpriseEditionModule extends CommunityEditionModule implements AbstractEnterpriseEditionModule
{
    private final ReconciledTransactionTracker reconciledTxTracker;
    private final FabricDatabaseManager fabricDatabaseManager;
    private final FabricServicesBootstrap fabricServicesBootstrap;
    private DatabaseStartAborter databaseStartAborter;

    public EnterpriseEditionModule( GlobalModule globalModule )
    {
        this( globalModule, globalModule.getGlobalDependencies() );
    }

    protected EnterpriseEditionModule( GlobalModule globalModule, Dependencies dependencies  )
    {
        super( globalModule );
        satisfyEnterpriseOnlyDependencies( globalModule );
        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig() );
        reconciledTxTracker = new DefaultReconciledTransactionTracker( globalModule.getLogService() );
        fabricServicesBootstrap = new FabricServicesBootstrap( globalModule.getGlobalLife(), dependencies, globalModule.getLogService() );
        fabricDatabaseManager = dependencies.resolveDependency( FabricDatabaseManager.class );
        SettingsWhitelist settingsWhiteList = new SettingsWhitelist( globalModule.getGlobalConfig() );
        dependencies.satisfyDependency( settingsWhiteList );
    }

    @Override
    public QueryEngineProvider getQueryEngineProvider()
    {
        return new EnterpriseCypherEngineProvider();
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager ) throws KernelException
    {
        super.registerEditionSpecificProcedures( globalProcedures, databaseManager );
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        fabricServicesBootstrap.registerProcedures( globalProcedures );
    }

    @Override
    protected Predicate<String> fileWatcherFileNameFilter()
    {
        return any( fileName -> fileName.startsWith( TransactionLogFilesHelper.DEFAULT_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }

    @Override
    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new EnterpriseConstraintSemantics();
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    @Override
    protected StatementLocksFactory createStatementLocksFactory( Locks locks, Config config, LogService logService )
    {
        return new StatementLocksFactorySelector( locks, config, logService ).select();
    }

    @Override
    protected Function<DatabaseId,TokenHolders> createTokenHolderProvider( GlobalModule platform )
    {
        Config globalConfig = platform.getGlobalConfig();
        return databaseId -> {
            DatabaseManager<?> databaseManager = platform.getGlobalDependencies().resolveDependency( DatabaseManager.class );
            Supplier<Kernel> kernelSupplier = () ->
            {
                DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseId )
                        .orElseThrow( () -> new IllegalStateException( format( "Database %s not found.", databaseId.name() ) ) );
                return databaseContext.dependencies().resolveDependency( Kernel.class );
            };
            return new TokenHolders(
                    new DelegatingTokenHolder( createPropertyKeyCreator( globalConfig, databaseId, kernelSupplier ), TYPE_PROPERTY_KEY ),
                    new DelegatingTokenHolder( createLabelIdCreator( globalConfig, databaseId, kernelSupplier ), TYPE_LABEL ),
                    new DelegatingTokenHolder( createRelationshipTypeCreator( globalConfig, databaseId, kernelSupplier ), TYPE_RELATIONSHIP_TYPE ) );
        };
    }

    @Override
    public DatabaseManager<?> createDatabaseManager( GlobalModule globalModule )
    {
        var databaseManager = new EnterpriseMultiDatabaseManager( globalModule, this );

        createDatabaseManagerDependentModules( databaseManager );

        globalModule.getGlobalLife().add( databaseManager );
        globalModule.getGlobalDependencies().satisfyDependency( databaseManager );

        Supplier<GraphDatabaseService> systemDbSupplier = () -> databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID )
                .orElseThrow()
                .databaseFacade();
        var dbmsModel = new EnterpriseSystemGraphDbmsModel( systemDbSupplier );
        databaseStartAborter = new DatabaseStartAborter( globalModule.getGlobalAvailabilityGuard(), dbmsModel, globalModule.getGlobalClock(),
                Duration.ofSeconds( 5 ) );
        StandaloneDbmsReconcilerModule reconcilerModule = new StandaloneDbmsReconcilerModule( globalModule, databaseManager, reconciledTxTracker, dbmsModel );
        databaseStateService = reconcilerModule.reconciler();
        globalModule.getGlobalLife().add( reconcilerModule );
        globalModule.getGlobalDependencies().satisfyDependency( reconciledTxTracker );

        return databaseManager;
    }

    private void createDatabaseManagerDependentModules( MultiDatabaseManager<StandaloneDatabaseContext> databaseManager )
    {
        initBackupIfNeeded( globalModule, globalModule.getGlobalConfig(), databaseManager );
    }

    @Override
    public SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        SystemGraphInitializer initializer = tryResolveOrCreate( SystemGraphInitializer.class, globalModule.getExternalDependencyResolver(),
                () -> new FabricSystemGraphInitializer( databaseManager, globalModule.getGlobalConfig(), fabricDatabaseManager ) );
        return globalModule.getGlobalDependencies().satisfyDependency( globalModule.getGlobalLife().add( initializer ) );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        SecurityProvider securityProvider;
        if ( globalModule.getGlobalConfig().get( GraphDatabaseSettings.auth_enabled ) )
        {
            EnterpriseSecurityModule securityModule = new EnterpriseSecurityModule(
                    globalModule.getLogService().getUserLogProvider(),
                    globalModule.getGlobalConfig(),
                    globalProcedures,
                    globalModule.getJobScheduler(),
                    globalModule.getFileSystem(),
                    globalModule.getGlobalDependencies(),
                    globalModule.getTransactionEventListeners()
            );
            securityModule.setup();
            globalModule.getGlobalLife().add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = EnterpriseNoAuthSecurityProvider.INSTANCE;
        }
        setSecurityProvider( securityProvider );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        FabricConfig fabricConfig = globalModule.getGlobalDependencies().resolveDependency( FabricConfig.class );
        return new FabricRoutingProcedureInstaller( databaseManager, portRegister, config, fabricDatabaseManager, fabricConfig, logProvider );
    }

    @Override
    public AuthManager getBoltAuthManager( DependencyResolver dependencyResolver )
    {
        AuthManager authManager = super.getBoltAuthManager( dependencyResolver );
        var fabricConfig = dependencyResolver.resolveDependency( FabricConfig.class );

        if ( !fabricConfig.isEnabled() )
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
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        FabricConfig config = dependencies.resolveDependency( FabricConfig.class );
        var kernelDatabaseManagementService = super.createBoltDatabaseManagementServiceProvider(dependencies, managementService, monitors, clock, logService);
        if ( !config.isEnabled() )
        {
            return kernelDatabaseManagementService;
        }

        FabricExecutor fabricExecutor = dependencies.resolveDependency( FabricExecutor.class );
        TransactionManager transactionManager = dependencies.resolveDependency( TransactionManager.class );
        FabricDatabaseManager fabricDatabaseManager = dependencies.resolveDependency( FabricDatabaseManager.class );

        var fabricDatabaseManagementService = new BoltFabricDatabaseManagementService( fabricExecutor, config, transactionManager, fabricDatabaseManager );

        return new BoltGraphDatabaseManagementServiceSPI()
        {

            @Override
            public BoltGraphDatabaseServiceSPI database( String databaseName ) throws UnavailableException, DatabaseNotFoundException
            {
                if ( fabricDatabaseManager.isFabricDatabase( databaseName ) )
                {
                    return fabricDatabaseManagementService.database( databaseName );
                }

                return kernelDatabaseManagementService.database( databaseName );
            }

            @Override
            public Optional<CustomBookmarkFormatParser> getCustomBookmarkFormatParser()
            {
                return fabricDatabaseManagementService.getCustomBookmarkFormatParser();
            }
        };
    }

    private void initBackupIfNeeded( GlobalModule globalModule, Config config, DatabaseManager<StandaloneDatabaseContext> databaseManager )
    {
        FileSystemAbstraction fs = globalModule.getFileSystem();
        JobScheduler jobScheduler = globalModule.getJobScheduler();
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();

        LogProvider internalLogProvider = globalModule.getLogService().getInternalLogProvider();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, internalLogProvider );
        PipelineBuilders pipelineBuilders = new PipelineBuilders( sslPolicyLoader );

        int maxChunkSize = config.get( CausalClusteringSettings.store_copy_chunk_size );
        TransactionBackupServiceProvider backupServiceProvider = new TransactionBackupServiceProvider(
                internalLogProvider, supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration(),
                supportedProtocolCreator.createSupportedModifierProtocols(),
                pipelineBuilders.backupServer(),
                new MultiDatabaseCatchupServerHandler( databaseManager, fs, maxChunkSize, internalLogProvider ),
                new InstalledProtocolHandler(),
                jobScheduler,
                portRegister
        );

        Optional<Server> backupServer = backupServiceProvider.resolveIfBackupEnabled( config );

        backupServer.ifPresent( globalModule.getGlobalLife()::add );
    }
}
