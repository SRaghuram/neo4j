/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.common.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.dbms.database.CommercialMultiDatabaseManager;
import com.neo4j.server.security.enterprise.systemgraph.CommercialSystemGraphInitializer;
import com.neo4j.kernel.enterprise.api.security.provider.CommercialNoAuthSecurityProvider;
import com.neo4j.kernel.impl.enterprise.CommercialConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.id.CommercialIdTypeConfigurationProvider;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.commercial.builtin.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.procedure.commercial.builtin.EnterpriseBuiltInProcedures;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.function.Predicates.any;

public class CommercialEditionModule extends CommunityEditionModule
{
    private final GlobalModule globalModule;

    public CommercialEditionModule( GlobalModule globalModule )
    {
        super( globalModule );
        this.globalModule = globalModule;
        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig() );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        super.registerEditionSpecificProcedures( globalProcedures );
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
    }

    @Override
    protected IdContextFactory createIdContextFactory( GlobalModule globalModule, FileSystemAbstraction fileSystem )
    {
        return IdContextFactoryBuilder.of( new CommercialIdTypeConfigurationProvider( globalModule.getGlobalConfig() ),
                globalModule.getJobScheduler() )
                .withFileSystem( fileSystem )
                .build();
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
        return new CommercialConstraintSemantics();
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
                    new DelegatingTokenHolder( createPropertyKeyCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_PROPERTY_KEY ),
                    new DelegatingTokenHolder( createLabelIdCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_LABEL ),
                    new DelegatingTokenHolder( createRelationshipTypeCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        };
    }

    @Override
    public DatabaseManager<?> createDatabaseManager( GlobalModule globalModule, Log msgLog )
    {
        CommercialMultiDatabaseManager databaseManager = new CommercialMultiDatabaseManager( globalModule, this, msgLog );
        createDatabaseManagerDependentModules( databaseManager );
        return databaseManager;
    }

    private void createDatabaseManagerDependentModules( DatabaseManager<StandaloneDatabaseContext> databaseManager )
    {
        initBackupIfNeeded( globalModule, globalModule.getGlobalConfig(), databaseManager );
    }

    @Override
    public void createDatabases( DatabaseManager<?> databaseManager, Config config ) throws DatabaseExistsException
    {
        createCommercialEditionDatabases( databaseManager, config );
    }

    private void createCommercialEditionDatabases( DatabaseManager<?> databaseManager, Config config )
            throws DatabaseExistsException
    {
        databaseManager.createDatabase( new DatabaseId( SYSTEM_DATABASE_NAME ) );
        createConfiguredDatabases( databaseManager, config );
    }

    private void createConfiguredDatabases( DatabaseManager<?> databaseManager, Config config ) throws DatabaseExistsException
    {
        databaseManager.createDatabase( new DatabaseId( config.get( GraphDatabaseSettings.default_database ) ) );
        DatabaseManagementService managementService = globalModule.getGlobalDependencies().resolveDependency( DatabaseManagementService.class );
        globalModule.getGlobalLife().addLifecycleListener( ( instance, from, to ) ->
        {
            if ( instance instanceof CompositeDatabaseAvailabilityGuard && LifecycleStatus.STARTED == to )
            {
                globalModule.getTransactionEventListeners().registerTransactionEventListener( SYSTEM_DATABASE_NAME,
                        new MultiDatabaseTransactionEventListener( databaseManager ) );
            }
        } );
        managementService.registerDatabaseEventListener( new SystemDatabaseEventListener( databaseManager, config ) );
    }

    @Override
    public SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        SystemGraphInitializer initializer = tryResolveOrCreate( SystemGraphInitializer.class, globalModule.getExternalDependencyResolver(),
                () -> new CommercialSystemGraphInitializer( databaseManager, globalModule.getGlobalConfig() ) );
        return globalModule.getGlobalDependencies().satisfyDependency( globalModule.getGlobalLife().add( initializer ) );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        SecurityProvider securityProvider;
        if ( globalModule.getGlobalConfig().get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule = setupSecurityModule( globalModule,
                    globalModule.getLogService().getUserLog( CommercialEditionModule.class ), globalProcedures, "commercial-security-module" );
            globalModule.getGlobalLife().add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = CommercialNoAuthSecurityProvider.INSTANCE;
        }
        setSecurityProvider( securityProvider );
    }

    private void initBackupIfNeeded( GlobalModule globalModule, Config config, DatabaseManager<StandaloneDatabaseContext> databaseManager )
    {
        FileSystemAbstraction fs = globalModule.getFileSystem();
        JobScheduler jobScheduler = globalModule.getJobScheduler();
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();

        LogProvider internalLogProvider = globalModule.getLogService().getInternalLogProvider();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, internalLogProvider );
        PipelineBuilders pipelineBuilders = new PipelineBuilders( SecurePipelineFactory::new, config, sslPolicyLoader );

        TransactionBackupServiceProvider backupServiceProvider = new TransactionBackupServiceProvider(
                internalLogProvider, supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration(),
                supportedProtocolCreator.createSupportedModifierProtocols(),
                pipelineBuilders.backupServer(),
                new MultiDatabaseCatchupServerHandler( databaseManager, internalLogProvider, fs ),
                new InstalledProtocolHandler(),
                jobScheduler,
                portRegister
        );

        Optional<Server> backupServer = backupServiceProvider.resolveIfBackupEnabled( config );

        backupServer.ifPresent( globalModule.getGlobalLife()::add );
    }

    private static class SystemDatabaseEventListener extends DatabaseEventListenerAdapter
    {
        private final DatabaseManager<?> databaseManager;
        private final Config config;

        SystemDatabaseEventListener( DatabaseManager<?> databaseManager, Config config )
        {
            this.databaseManager = databaseManager;
            this.config = config;
        }

        @Override
        public void databaseStart( DatabaseEventContext eventContext )
        {
            if ( SYSTEM_DATABASE_NAME.equals( eventContext.getDatabaseName() ) )
            {
                MultiDatabaseTransactionEventListener.createDatabasesFromSystem( databaseManager, config );
            }
        }
    }
}
