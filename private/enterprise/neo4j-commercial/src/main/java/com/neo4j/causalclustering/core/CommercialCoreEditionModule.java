/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CommercialCatchupServerHandler;
import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import com.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;
import com.neo4j.security.CommercialSecurityModule;

import java.time.Clock;

import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.EnterpriseCoreEditionModule;
import org.neo4j.causalclustering.core.IdentityModule;
import org.neo4j.causalclustering.core.state.ClusteringModule;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.CoreStateStorageService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.ssl.SslPolicy;

import static com.neo4j.security.configuration.CommercialSecuritySettings.isSystemDatabaseEnabled;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class CommercialCoreEditionModule extends EnterpriseCoreEditionModule
{
    private final GlobalTransactionStats globalTransactionStats;
    private DatabaseInitializer securityDatabaseInitializer;

    CommercialCoreEditionModule( final PlatformModule platformModule, final SslDiscoveryServiceFactory discoveryServiceFactory )
    {
        super( platformModule, discoveryServiceFactory );
        this.globalTransactionStats = new GlobalTransactionStats();
        initGlobalGuard( platformModule.clock, platformModule.logService );
    }

    @Override
    protected CatchupServerHandler getHandlerFactory( PlatformModule platformModule,
            FileSystemAbstraction fileSystem, CoreSnapshotService snapshotService )
    {
        return new CommercialCatchupServerHandler( databaseService, logProvider, fileSystem, snapshotService );
    }

    @Override
    protected ClusteringModule getClusteringModule( PlatformModule platformModule, DiscoveryServiceFactory discoveryServiceFactory,
            CoreStateStorageService storage, IdentityModule identityModule, Dependencies dependencies )
    {
        SslPolicyLoader sslPolicyFactory = dependencies.resolveDependency( SslPolicyLoader.class );
        SslPolicy clusterSslPolicy = sslPolicyFactory.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );

        if ( discoveryServiceFactory instanceof SslDiscoveryServiceFactory )
        {
            ((SslDiscoveryServiceFactory) discoveryServiceFactory).setSslPolicy( clusterSslPolicy );
        }

        return new ClusteringModule( discoveryServiceFactory, identityModule.myself(), platformModule, storage, databaseService, databaseInitializers );
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
        if ( isSystemDatabaseEnabled( config ) )
        {
            createDatabase( databaseManager, GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
            assert databaseInitializers.get( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) == null;

            if ( securityDatabaseInitializer != null )
            {
                databaseInitializers.put( GraphDatabaseSettings.SYSTEM_DATABASE_NAME, securityDatabaseInitializer );
            }
        }
        createConfiguredDatabases( databaseManager, config );
    }

    private void createConfiguredDatabases( DatabaseManager databaseManager, Config config )
    {
        createDatabase( databaseManager, config.get( GraphDatabaseSettings.active_database ) );
    }

    @Override
    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
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
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        SecurityProvider securityProvider;
        if ( platformModule.config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            CommercialSecurityModule securityModule = (CommercialSecurityModule) setupSecurityModule( platformModule, this,
                    platformModule.logService.getUserLog( CommercialCoreEditionModule.class ), procedures, "commercial-security-module" );
            securityDatabaseInitializer = securityModule.markForExternalBootstrappingAndGetInitializer();
            platformModule.life.add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = EnterpriseNoAuthSecurityProvider.INSTANCE;
        }
        setSecurityProvider( securityProvider );
    }

    private void initGlobalGuard( Clock clock, LogService logService )
    {
        if ( globalAvailabilityGuard == null )
        {
            globalAvailabilityGuard = new CompositeDatabaseAvailabilityGuard( clock, logService );
        }
    }
}
