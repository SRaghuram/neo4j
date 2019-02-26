/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.core.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import com.neo4j.kernel.enterprise.api.security.provider.CommercialNoAuthSecurityProvider;
import com.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import com.neo4j.kernel.impl.enterprise.CommercialConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.id.CommercialIdTypeConfigurationProvider;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import com.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;

import java.time.Clock;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConnectorPortRegister;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;
import static org.neo4j.function.Predicates.any;

public class CommercialEditionModule extends CommunityEditionModule
{
    private final GlobalTransactionStats globalTransactionStats;

    public CommercialEditionModule( GlobalModule globalModule )
    {
        super( globalModule );
        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig() );
        globalTransactionStats = new GlobalTransactionStats();
        initGlobalGuard( globalModule.getGlobalClock(), globalModule.getLogService() );
        initBackupIfNeeded( globalModule, globalModule.getGlobalConfig() );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
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
        return any( fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ), filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
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
    protected Function<String,TokenHolders> createTokenHolderProvider( GlobalModule platform )
    {
        Config globalConfig = platform.getGlobalConfig();
        return databaseName -> {
            DatabaseManager databaseManager = platform.getGlobalDependencies().resolveDependency( DatabaseManager.class );
            Supplier<Kernel> kernelSupplier = () ->
            {
                DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseName )
                        .orElseThrow( () -> new IllegalStateException( format( "Database %s not found.", databaseName ) ) );
                return databaseContext.getDependencies().resolveDependency( Kernel.class );
            };
            return new TokenHolders(
                    new DelegatingTokenHolder( createPropertyKeyCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_PROPERTY_KEY ),
                    new DelegatingTokenHolder( createLabelIdCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_LABEL ),
                    new DelegatingTokenHolder( createRelationshipTypeCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        };
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

    private static void createCommercialEditionDatabases( DatabaseManager databaseManager, Config config )
    {
        databaseManager.createDatabase( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        createConfiguredDatabases( databaseManager, config );
    }

    private static void createConfiguredDatabases( DatabaseManager databaseManager, Config config )
    {
        databaseManager.createDatabase( config.get( GraphDatabaseSettings.active_database ) );
    }

    @Override
    public DatabaseTransactionStats createTransactionMonitor()
    {
        return globalTransactionStats.createDatabaseTransactionMonitor();
    }

    @Override
    public TransactionCounters globalTransactionCounter()
    {
        return globalTransactionStats;
    }

    @Override
    public AvailabilityGuard getGlobalAvailabilityGuard( Clock clock, LogService logService, Config config )
    {
        initGlobalGuard( clock, logService );
        return globalAvailabilityGuard;
    }

    @Override
    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( String databaseName, Clock clock, LogService logService, Config config )
    {
        return ((CompositeDatabaseAvailabilityGuard) getGlobalAvailabilityGuard( clock, logService, config )).createDatabaseAvailabilityGuard( databaseName );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule, GlobalProcedures globalProcedures )
    {
        createCommercialSecurityModule( this, globalModule, globalProcedures );
    }

    private static void createCommercialSecurityModule( AbstractEditionModule editionModule, GlobalModule globalModule, GlobalProcedures globalProcedures )
    {
        SecurityProvider securityProvider;
        if ( globalModule.getGlobalConfig().get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule = setupSecurityModule( globalModule, editionModule,
                    globalModule.getLogService().getUserLog( CommercialEditionModule.class ), globalProcedures, "commercial-security-module" );
            globalModule.getGlobalLife().add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = CommercialNoAuthSecurityProvider.INSTANCE;
        }
        editionModule.setSecurityProvider( securityProvider );
    }

    private void initGlobalGuard( Clock clock, LogService logService )
    {
        if ( globalAvailabilityGuard == null )
        {
            globalAvailabilityGuard = new CompositeDatabaseAvailabilityGuard( clock, logService );
        }
    }

    private void initBackupIfNeeded( GlobalModule globalModule, Config config )
    {
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Supplier<DatabaseManager> databaseManagerSupplier = globalDependencies.provideDependency( DatabaseManager.class );
        FileSystemAbstraction fs = globalModule.getFileSystem();
        JobScheduler jobScheduler = globalModule.getJobScheduler();
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();

        LogProvider internalLogProvider = globalModule.getLogService().getInternalLogProvider();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, internalLogProvider );
        PipelineBuilders pipelineBuilders = new PipelineBuilders( SecurePipelineFactory::new, internalLogProvider, config, globalDependencies );

        TransactionBackupServiceProvider backupServiceProvider = new TransactionBackupServiceProvider(
                internalLogProvider, supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration(),
                supportedProtocolCreator.createSupportedModifierProtocols(),
                pipelineBuilders.backupServer(),
                new MultiDatabaseCatchupServerHandler( databaseManagerSupplier, internalLogProvider, fs ),
                new InstalledProtocolHandler(),
                config.get( GraphDatabaseSettings.active_database ),
                jobScheduler,
                portRegister
        );

        Optional<Server> backupServer = backupServiceProvider.resolveIfBackupEnabled( config );

        backupServer.ifPresent( globalModule.getGlobalLife()::add );
    }
}
