/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bootstrap;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.configuration.FabricEnterpriseConfig;
import com.neo4j.configuration.FabricEnterpriseSettings;
import com.neo4j.fabric.auth.ClusterCredentialsProvider;
import com.neo4j.fabric.auth.CredentialsProvider;
import com.neo4j.fabric.auth.ExternalCredentialsProvider;
import com.neo4j.fabric.driver.ClusterDriverConfigFactory;
import com.neo4j.fabric.driver.DriverConfigFactory;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.ExternalDriverConfigFactory;
import com.neo4j.fabric.eval.ClusterCatalogManager;
import com.neo4j.fabric.eval.EnterpriseSingleCatalogManager;
import com.neo4j.fabric.eval.LeaderLookup;
import com.neo4j.fabric.executor.FabricDatabaseAccessImpl;
import com.neo4j.fabric.executor.FabricRemoteExecutorImpl;
import com.neo4j.fabric.functions.GraphIdsFunction;
import com.neo4j.fabric.localdb.FabricEnterpriseDatabaseManager;

import java.time.Clock;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.FabricDatabaseAccess;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

public abstract class EnterpriseFabricServicesBootstrap extends FabricServicesBootstrap
{
    public EnterpriseFabricServicesBootstrap( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
    {
        super( lifeSupport, dependencies, logService );
    }

    @Override
    protected FabricRemoteExecutor bootstrapRemoteStack()
    {
        var credentialsProvider = register( createCredentialsProvider(), CredentialsProvider.class );

        var fabricConfig = resolve( FabricEnterpriseConfig.class );
        var config = resolve( Config.class );
        var sslPolicyLoader = resolve( SslPolicyLoader.class );
        var driverConfigFactory = createDriverConfigFactory( fabricConfig, config, sslPolicyLoader );

        var jobScheduler = resolve( JobScheduler.class );
        var driverPool =
                register( new DriverPool( jobScheduler, driverConfigFactory, fabricConfig, Clock.systemUTC(), credentialsProvider ), DriverPool.class );
        return register( new FabricRemoteExecutorImpl( driverPool ), FabricRemoteExecutor.class );
    }

    protected FabricEnterpriseConfig getFabricConfig()
    {
        return resolve( FabricEnterpriseConfig.class );
    }

    @Override
    protected FabricConfig bootstrapFabricConfig()
    {
        var config = resolve( Config.class );
        validateFabricSettings( config );

        return register( FabricEnterpriseConfig.from( config ), FabricEnterpriseConfig.class );
    }

    protected abstract DriverConfigFactory createDriverConfigFactory( FabricEnterpriseConfig fabricConfig, Config serverConfig,
            SslPolicyLoader sslPolicyLoader );

    protected abstract CredentialsProvider createCredentialsProvider();

    protected abstract void validateFabricSettings( Config config );

    public void registerProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        var fabricConfig = resolve( FabricEnterpriseConfig.class );
        if ( fabricConfig.getDatabase() != null )
        {
            globalProcedures.registerBuiltIn( new GraphIdsFunction( fabricConfig ) );
        }
    }

    public static class Single extends EnterpriseFabricServicesBootstrap
    {
        private final LogService logService;

        public Single( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
            this.logService = logService;
        }

        @Override
        protected void validateFabricSettings( Config config )
        {
            // any valid fabric config is valid in the context of single instance
        }

        @Override
        protected FabricDatabaseManager createFabricDatabaseManager( FabricConfig fabricConfig )
        {
            var databaseManager = (DatabaseManager<DatabaseContext>) resolve( DatabaseManager.class );
            return new FabricEnterpriseDatabaseManager.Single( (FabricEnterpriseConfig) fabricConfig, databaseManager, logService.getInternalLogProvider() );
        }

        @Override
        protected CatalogManager createCatalogManger()
        {
            var databaseManagementService = resolve( DatabaseManagementService.class);
            return new EnterpriseSingleCatalogManager( createDatabaseLookup(), databaseManagementService, getFabricConfig() );
        }

        @Override
        protected DriverConfigFactory createDriverConfigFactory( FabricEnterpriseConfig fabricConfig, Config serverConfig, SslPolicyLoader sslPolicyLoader )
        {
            return new ExternalDriverConfigFactory( fabricConfig, serverConfig, sslPolicyLoader );
        }

        @Override
        protected CredentialsProvider createCredentialsProvider()
        {
            return new ExternalCredentialsProvider();
        }

        @Override
        protected FabricDatabaseAccess createFabricDatabaseAccess()
        {
            return new FabricDatabaseAccessImpl( resolve( FabricDatabaseManager.class ) );
        }
    }

    public abstract static class ClusterMember extends EnterpriseFabricServicesBootstrap
    {

        public ClusterMember( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
        }

        @Override
        protected void validateFabricSettings( Config config )
        {
            assertNotSet( config, FabricEnterpriseSettings.database_name );
            assertNotSet( config, FabricEnterpriseSettings.fabric_servers_setting );
            assertNotSet( config, FabricEnterpriseSettings.routing_ttl_setting );

            if ( !config.getGroups( FabricEnterpriseSettings.GraphSetting.class ).isEmpty() )
            {
                unsupportedSetting( "fabric.graph" );
            }
        }

        private void assertNotSet( Config config, Setting<?> setting )
        {
            if ( config.isExplicitlySet( setting ) )
            {
                unsupportedSetting( setting.name() );
            }
        }

        private void unsupportedSetting( String settingName )
        {
            throw new IllegalArgumentException( "Setting '" + settingName + "' not supported on cluster instances" );
        }

        @Override
        protected FabricDatabaseManager createFabricDatabaseManager( FabricConfig fabricConfig )
        {
            var databaseManager = (DatabaseManager<DatabaseContext>) resolve( DatabaseManager.class );
            return new FabricEnterpriseDatabaseManager.Cluster( (FabricEnterpriseConfig) fabricConfig, databaseManager );
        }

        @Override
        protected CatalogManager createCatalogManger()
        {
            var fabricConfig = resolve( FabricEnterpriseConfig.class );
            var databaseManagementService = resolve( DatabaseManagementService.class );
            var topologyService = resolve( TopologyService.class );
            var leaderLookup = leaderLookup( topologyService );

            return new ClusterCatalogManager( createDatabaseLookup(), databaseManagementService, leaderLookup, fabricConfig, fabricConfig.isRoutingEnabled() );
        }

        protected abstract LeaderLookup leaderLookup( TopologyService topologyService );

        @Override
        protected DriverConfigFactory createDriverConfigFactory( FabricEnterpriseConfig fabricConfig, Config serverConfig, SslPolicyLoader sslPolicyLoader )
        {
            return new ClusterDriverConfigFactory( fabricConfig, serverConfig, sslPolicyLoader );
        }

        @Override
        protected CredentialsProvider createCredentialsProvider()
        {
            return new ClusterCredentialsProvider();
        }

        @Override
        protected FabricDatabaseAccess createFabricDatabaseAccess()
        {
            return FabricDatabaseAccess.NO_RESTRICTION;
        }
    }

    public static class Core extends ClusterMember
    {

        public Core( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
        }

        @Override
        protected LeaderLookup leaderLookup( TopologyService topologyService )
        {
            var leaderService = resolve( LeaderService.class );
            return new LeaderLookup.Core( topologyService, leaderService );
        }
    }

    public static class ReadReplica extends ClusterMember
    {
        public ReadReplica( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
        }

        @Override
        protected LeaderLookup leaderLookup( TopologyService topologyService )
        {
            return new LeaderLookup.ReadReplica( topologyService );
        }
    }
}
