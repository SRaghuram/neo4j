/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bootstrap;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.fabric.auth.CredentialsProvider;
import com.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;
import com.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import com.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.config.FabricSettings;
import com.neo4j.fabric.driver.ClusterDriverConfigFactory;
import com.neo4j.fabric.driver.DriverConfigFactory;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.ExternalDriverConfigFactory;
import com.neo4j.fabric.eval.CatalogManager;
import com.neo4j.fabric.eval.ClusterCatalogManager;
import com.neo4j.fabric.eval.DatabaseLookup;
import com.neo4j.fabric.eval.LeaderLookup;
import com.neo4j.fabric.eval.SingleCatalogManager;
import com.neo4j.fabric.eval.UseEvaluation;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.executor.FabricLocalExecutor;
import com.neo4j.fabric.executor.FabricQueryMonitoring;
import com.neo4j.fabric.executor.FabricRemoteExecutor;
import com.neo4j.fabric.functions.GraphIdsFunction;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.pipeline.SignatureResolver;
import com.neo4j.fabric.planning.FabricPlanner;
import com.neo4j.fabric.transaction.TransactionManager;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.txtracking.SimpleReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.CypherConfiguration;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.scheduler.Group.FABRIC_WORKER;

public abstract class FabricServicesBootstrap
{

    private final FabricConfig fabricConfig;
    private final Dependencies dependencies;
    private final LogService logService;
    private final ServiceBootstrapper serviceBootstrapper;
    private final Config config;

    public FabricServicesBootstrap( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
    {
        this.dependencies = dependencies;
        this.logService = logService;

        serviceBootstrapper = new ServiceBootstrapper( lifeSupport, dependencies );

        config = dependencies.resolveDependency( Config.class );

        validateFabricConfig( config );

        fabricConfig = serviceBootstrapper.registerService( FabricConfig.from( config ), FabricConfig.class );
    }

    public void bootstrapServices()
    {
        LogProvider internalLogProvider = logService.getInternalLogProvider();

        var fabricDatabaseManager =
                serviceBootstrapper.registerService( createFabricDatabaseManager( fabricConfig, dependencies, logService ), FabricDatabaseManager.class );

        var jobScheduler = dependencies.resolveDependency( JobScheduler.class );
        var monitors = dependencies.resolveDependency( Monitors.class );
        var proceduresSupplier = dependencies.provideDependency( GlobalProcedures.class );

        var credentialsProvider = serviceBootstrapper.registerService( new CredentialsProvider(), CredentialsProvider.class );

        var sslPolicyLoader = dependencies.resolveDependency( SslPolicyLoader.class );
        var driverConfigFactory = createDriverConfigFactory( fabricConfig, config, sslPolicyLoader );

        var driverPool = serviceBootstrapper.registerService(
                new DriverPool( jobScheduler, driverConfigFactory, fabricConfig, Clock.systemUTC(), credentialsProvider ), DriverPool.class );
        serviceBootstrapper.registerService( new FabricRemoteExecutor( driverPool ), FabricRemoteExecutor.class );
        serviceBootstrapper.registerService( new FabricLocalExecutor( fabricConfig, fabricDatabaseManager ), FabricLocalExecutor.class );
        serviceBootstrapper.registerService( new TransactionManager( dependencies ), TransactionManager.class );

        var cypherConfig = CypherConfiguration.fromConfig( config );

        var catalogManager = serviceBootstrapper.registerService( createCatalogManger( fabricConfig, dependencies ), CatalogManager.class );
        var signatureResolver = new SignatureResolver( proceduresSupplier );
        var monitoring = new FabricQueryMonitoring( dependencies, monitors );
        var planner = serviceBootstrapper.registerService( new FabricPlanner( fabricConfig, cypherConfig, monitors, signatureResolver ), FabricPlanner.class );
        var useEvaluation =
                serviceBootstrapper.registerService( new UseEvaluation( catalogManager, proceduresSupplier, signatureResolver ), UseEvaluation.class );

        serviceBootstrapper.registerService( new FabricReactorHooksService( internalLogProvider ), FabricReactorHooksService.class );

        Executor fabricWorkerExecutor = jobScheduler.executor( FABRIC_WORKER );
        var fabricExecutor = new FabricExecutor( fabricConfig, planner, useEvaluation, catalogManager, internalLogProvider, monitoring, fabricWorkerExecutor );
        serviceBootstrapper.registerService( fabricExecutor, FabricExecutor.class );

        serviceBootstrapper.registerService( new TransactionBookmarkManagerFactory( fabricDatabaseManager ), TransactionBookmarkManagerFactory.class );
    }

    protected DatabaseLookup createDatabaseLookup( Dependencies dependencies )
    {
        Supplier<DatabaseManager<DatabaseContext>> databaseManagerProvider = () ->
                (DatabaseManager<DatabaseContext>) dependencies.resolveDependency( DatabaseManager.class );
        return new DatabaseLookup.Default( databaseManagerProvider );
    }

    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider(
            BoltGraphDatabaseManagementServiceSPI kernelDatabaseManagementService, DatabaseManagementService managementService, Monitors monitors,
            SystemNanoClock clock )
    {
        FabricExecutor fabricExecutor = dependencies.resolveDependency( FabricExecutor.class );
        TransactionManager transactionManager = dependencies.resolveDependency( TransactionManager.class );
        FabricDatabaseManager fabricDatabaseManager = dependencies.resolveDependency( FabricDatabaseManager.class );

        var serverConfig = dependencies.resolveDependency( Config.class );
        var bookmarkTimeout = serverConfig.get( GraphDatabaseSettings.bookmark_ready_timeout );
        var reconciledTxTracker = new SimpleReconciledTransactionTracker( managementService, logService );

        var transactionIdTracker = new TransactionIdTracker( managementService, reconciledTxTracker, monitors, clock );

        var databaseManager = (DatabaseManager<DatabaseContext>) dependencies.resolveDependency( DatabaseManager.class );
        var databaseIdRepository = databaseManager.databaseIdRepository();
        var transactionBookmarkManagerFactory = dependencies.resolveDependency( TransactionBookmarkManagerFactory.class );

        var localGraphTransactionIdTracker = new LocalGraphTransactionIdTracker( transactionIdTracker, databaseIdRepository, bookmarkTimeout );
        var fabricDatabaseManagementService = dependencies.satisfyDependency(
                new BoltFabricDatabaseManagementService( fabricExecutor, fabricConfig, transactionManager, fabricDatabaseManager,
                                                         localGraphTransactionIdTracker, transactionBookmarkManagerFactory ) );

        return new BoltGraphDatabaseManagementServiceSPI()
        {

            @Override
            public BoltGraphDatabaseServiceSPI database( String databaseName ) throws UnavailableException, DatabaseNotFoundException
            {
                if ( fabricDatabaseManager.hasMultiGraphCapabilities( databaseName ) )
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

    protected abstract void validateFabricConfig( Config config );

    protected abstract FabricDatabaseManager createFabricDatabaseManager( FabricConfig fabricConfig, Dependencies dependencies, LogService logService );

    protected abstract CatalogManager createCatalogManger( FabricConfig fabricConfig, Dependencies dependencies );

    protected abstract DriverConfigFactory createDriverConfigFactory( FabricConfig fabricConfig, Config serverConfig, SslPolicyLoader sslPolicyLoader );

    public void registerProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        if ( fabricConfig.getDatabase() != null )
        {
            globalProcedures.register( new GraphIdsFunction( fabricConfig ) );
        }
    }

    public static class Single extends FabricServicesBootstrap
    {

        public Single( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
        }

        @Override
        protected void validateFabricConfig( Config config )
        {
            // any valid fabric config is valid in the context of single instance
        }

        @Override
        protected FabricDatabaseManager createFabricDatabaseManager( FabricConfig fabricConfig, Dependencies dependencies, LogService logService )
        {
            var databaseManager = (DatabaseManager<DatabaseContext>) dependencies.resolveDependency( DatabaseManager.class );
            return new FabricDatabaseManager.Single( fabricConfig, databaseManager, logService.getInternalLogProvider() );
        }

        @Override
        protected CatalogManager createCatalogManger( FabricConfig fabricConfig, Dependencies dependencies )
        {

            return new SingleCatalogManager( createDatabaseLookup( dependencies ), fabricConfig );
        }

        @Override
        protected DriverConfigFactory createDriverConfigFactory( FabricConfig fabricConfig, Config serverConfig, SslPolicyLoader sslPolicyLoader )
        {
            return new ExternalDriverConfigFactory( fabricConfig, serverConfig, sslPolicyLoader );
        }
    }

    public abstract static class ClusterMember extends FabricServicesBootstrap
    {

        public ClusterMember( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
        }

        @Override
        protected void validateFabricConfig( Config config )
        {
            assertNotSet( config, FabricSettings.databaseName );
            assertNotSet( config, FabricSettings.fabricServersSetting );
            assertNotSet( config, FabricSettings.routingTtlSetting );

            if ( !config.getGroups( FabricSettings.GraphSetting.class ).isEmpty() )
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
        protected FabricDatabaseManager createFabricDatabaseManager( FabricConfig fabricConfig, Dependencies dependencies, LogService logService )
        {
            var databaseManager = (DatabaseManager<DatabaseContext>) dependencies.resolveDependency( DatabaseManager.class );
            return new FabricDatabaseManager.Cluster( databaseManager );
        }

        @Override
        protected DriverConfigFactory createDriverConfigFactory( FabricConfig fabricConfig, Config serverConfig, SslPolicyLoader sslPolicyLoader )
        {
            return new ClusterDriverConfigFactory();
        }
    }

    public static class Core extends ClusterMember
    {

        public Core( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
        }

        @Override
        protected CatalogManager createCatalogManger( FabricConfig fabricConfig, Dependencies dependencies )
        {
            var topologyService = dependencies.resolveDependency( TopologyService.class );
            var leaderService = dependencies.resolveDependency( LeaderService.class );
            var leaderLookup = new LeaderLookup.Core( topologyService, leaderService );

            return new ClusterCatalogManager( createDatabaseLookup( dependencies ), leaderLookup, fabricConfig );
        }
    }

    public static class ReadReplica extends ClusterMember
    {
        public ReadReplica( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
        }

        @Override
        protected CatalogManager createCatalogManger( FabricConfig fabricConfig, Dependencies dependencies )
        {
            var topologyService = dependencies.resolveDependency( TopologyService.class );
            var leaderLookup = new LeaderLookup.ReadReplica( topologyService );

            return new ClusterCatalogManager( createDatabaseLookup( dependencies ), leaderLookup, fabricConfig );
        }
    }

    private static class ServiceBootstrapper
    {
        private final LifeSupport lifeSupport;
        private final Dependencies dependencies;

        ServiceBootstrapper( LifeSupport lifeSupport, Dependencies dependencies )
        {
            this.lifeSupport = lifeSupport;
            this.dependencies = dependencies;
        }

        <T> T registerService( T dependency, Class<T> dependencyType )
        {
            dependencies.satisfyDependency( dependency );

            if ( LifecycleAdapter.class.isAssignableFrom( dependencyType ) )
            {
                lifeSupport.add( (LifecycleAdapter) dependency );
            }

            return dependencies.resolveDependency( dependencyType );
        }
    }
}
