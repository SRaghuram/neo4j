/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition;

import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.common.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.dbms.StandaloneDbmsReconcilerModule;
import com.neo4j.dbms.database.EnterpriseMultiDatabaseManager;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.localdb.FabricSystemGraphInitializer;
import com.neo4j.fabric.routing.FabricRoutingProcedureInstaller;
import com.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import com.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInProcedures;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.cypher.internal.javacompat.EnterpriseCypherEngineProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseId;
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
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

public class EnterpriseEditionModule extends CommunityEditionModule implements AbstractEnterpriseEditionModule
{
    private final GlobalModule globalModule;
    private final ReconciledTransactionTracker reconciledTxTracker;
    private final FabricDatabaseManager fabricDatabaseManager;

    public EnterpriseEditionModule( GlobalModule globalModule )
    {
        this( globalModule, globalModule.getGlobalDependencies() );
    }

    public EnterpriseEditionModule( GlobalModule globalModule, Dependencies dependencies  )
    {
        super( globalModule );
        this.globalModule = globalModule;
        satisfyEnterpriseOnlyDependencies( this.globalModule );
        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig() );
        reconciledTxTracker = new DefaultReconciledTransactionTracker( globalModule.getLogService() );
        new FabricServicesBootstrap( globalModule.getGlobalLife(), dependencies );
        fabricDatabaseManager = dependencies.resolveDependency( FabricDatabaseManager.class );
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

        var reconcilerModule = new StandaloneDbmsReconcilerModule<>( globalModule, databaseManager, reconciledTxTracker );
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
            SecurityModule securityModule =
                    setupSecurityModule( globalModule,
                                         globalModule.getLogService().getUserLog( EnterpriseEditionModule.class ),
                                         globalProcedures, "enterprise-security-module" );
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
        return new FabricRoutingProcedureInstaller( databaseManager, portRegister, config, fabricDatabaseManager, logProvider );
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var kernelDatabaseManagementService = super.createBoltDatabaseManagementServiceProvider(dependencies, managementService, monitors, clock, logService);

        return databaseName ->
        {
            if ( fabricDatabaseManager.isFabricDatabase( databaseName ) )
            {
                // TODO: this is where Fabric logic gets plugged in
                // return fabricDatabaseManagementService.database( databaseName );
                throw new IllegalStateException( "Fabric is not here yet" );
            }

            return kernelDatabaseManagementService.database( databaseName );
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

        TransactionBackupServiceProvider backupServiceProvider = new TransactionBackupServiceProvider(
                internalLogProvider, supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration(),
                supportedProtocolCreator.createSupportedModifierProtocols(),
                pipelineBuilders.backupServer(),
                new MultiDatabaseCatchupServerHandler( databaseManager, fs, internalLogProvider ),
                new InstalledProtocolHandler(),
                jobScheduler,
                portRegister
        );

        Optional<Server> backupServer = backupServiceProvider.resolveIfBackupEnabled( config );

        backupServer.ifPresent( globalModule.getGlobalLife()::add );
    }
}
