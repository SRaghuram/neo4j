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
import com.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import com.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import com.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;

import java.time.Clock;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;

public class CommercialEditionModule extends EnterpriseEditionModule
{
    private final GlobalTransactionStats globalTransactionStats;

    public CommercialEditionModule( PlatformModule platformModule )
    {
        super( platformModule );
        globalTransactionStats = new GlobalTransactionStats();
        initGlobalGuard( platformModule.clock, platformModule.logService );
        initBackupIfNeeded( platformModule, platformModule.config );
    }

    protected Function<String,TokenHolders> createTokenHolderProvider( PlatformModule platform )
    {
        Config config = platform.config;
        return databaseName -> {
            DatabaseManager databaseManager = platform.dependencies.resolveDependency( DatabaseManager.class );
            Supplier<Kernel> kernelSupplier = () ->
            {
                DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseName )
                        .orElseThrow( () -> new IllegalStateException( format( "Database %s not found.", databaseName ) ) );
                return databaseContext.getDependencies().resolveDependency( Kernel.class );
            };
            return new TokenHolders(
                    new DelegatingTokenHolder( createPropertyKeyCreator( config, kernelSupplier ), TokenHolder.TYPE_PROPERTY_KEY ),
                    new DelegatingTokenHolder( createLabelIdCreator( config, kernelSupplier ), TokenHolder.TYPE_LABEL ),
                    new DelegatingTokenHolder( createRelationshipTypeCreator( config, kernelSupplier ), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        };
    }

    @Override
    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, PlatformModule platform, AbstractEditionModule edition,
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
    public void createSecurityModule( PlatformModule platformModule, GlobalProcedures globalProcedures )
    {
        createCommercialSecurityModule( this, platformModule, globalProcedures );
    }

    private static void createCommercialSecurityModule( AbstractEditionModule editionModule, PlatformModule platformModule, GlobalProcedures globalProcedures )
    {
        SecurityProvider securityProvider;
        if ( platformModule.config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule = setupSecurityModule( platformModule, editionModule,
                    platformModule.logService.getUserLog( EnterpriseEditionModule.class ), globalProcedures, "commercial-security-module" );
            platformModule.life.add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = EnterpriseNoAuthSecurityProvider.INSTANCE;
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

    private void initBackupIfNeeded( PlatformModule platformModule, Config config )
    {
        Dependencies dependencies = platformModule.dependencies;
        Supplier<DatabaseManager> databaseManagerSupplier = dependencies.provideDependency( DatabaseManager.class );
        FileSystemAbstraction fs = platformModule.fileSystem;
        JobScheduler jobScheduler = platformModule.jobScheduler;
        ConnectorPortRegister portRegister = platformModule.connectorPortRegister;

        LogProvider internalLogProvider = platformModule.logService.getInternalLogProvider();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, internalLogProvider );
        PipelineBuilders pipelineBuilders = new PipelineBuilders( SecurePipelineFactory::new, internalLogProvider, config, dependencies );

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

        backupServer.ifPresent( platformModule.life::add );
    }
}
