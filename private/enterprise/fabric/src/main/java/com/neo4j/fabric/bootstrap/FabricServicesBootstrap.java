/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bootstrap;

import com.neo4j.fabric.auth.CredentialsProvider;
import com.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.eval.CatalogManager;
import com.neo4j.fabric.eval.DatabaseLookup;
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
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.CypherConfiguration;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

import static org.neo4j.scheduler.Group.FABRIC_WORKER;

public class FabricServicesBootstrap
{

    private final FabricConfig fabricConfig;

    public FabricServicesBootstrap( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
    {
        var serviceBootstrapper = new ServiceBootstrapper( lifeSupport, dependencies );

        LogProvider internalLogProvider = logService.getInternalLogProvider();
        var config = dependencies.resolveDependency( Config.class );

        fabricConfig = serviceBootstrapper
                .registerService( FabricConfig.from( config ), FabricConfig.class );
        var fabricDatabaseManager = serviceBootstrapper
                .registerService( new FabricDatabaseManager( fabricConfig, dependencies, internalLogProvider ), FabricDatabaseManager.class );

        if ( fabricConfig.isEnabled() )
        {
            Supplier<DatabaseManager<DatabaseContext>> databaseManagerProvider = () ->
                    (DatabaseManager<DatabaseContext>) dependencies.resolveDependency( DatabaseManager.class );
            var jobScheduler = dependencies.resolveDependency( JobScheduler.class );
            var monitors = dependencies.resolveDependency( Monitors.class );
            var proceduresSupplier = dependencies.provideDependency( GlobalProcedures.class );

            var credentialsProvider = serviceBootstrapper
                    .registerService( new CredentialsProvider(), CredentialsProvider.class );

            var sslPolicyLoader = dependencies.resolveDependency( SslPolicyLoader.class );
            var driverPool = serviceBootstrapper
                    .registerService( new DriverPool( jobScheduler, fabricConfig, config, Clock.systemUTC(), credentialsProvider, sslPolicyLoader ),
                                      DriverPool.class );
            serviceBootstrapper
                    .registerService( new FabricRemoteExecutor( driverPool ), FabricRemoteExecutor.class );
            serviceBootstrapper
                    .registerService( new FabricLocalExecutor( fabricConfig, fabricDatabaseManager ), FabricLocalExecutor.class );
            serviceBootstrapper
                    .registerService( new TransactionManager( dependencies ), TransactionManager.class );

            var cypherConfig = CypherConfiguration.fromConfig( config );
            var databaseLookup = new DatabaseLookup.Default( databaseManagerProvider );
            var catalogManager = serviceBootstrapper
                    .registerService( new SingleCatalogManager( databaseLookup, fabricConfig ), CatalogManager.class );
            var signatureResolver = new SignatureResolver( proceduresSupplier );
            var monitoring = new FabricQueryMonitoring( dependencies, monitors );
            var planner = serviceBootstrapper
                    .registerService( new FabricPlanner( fabricConfig, cypherConfig, monitors, signatureResolver ), FabricPlanner.class );
            var useEvaluation = serviceBootstrapper
                    .registerService( new UseEvaluation( catalogManager, proceduresSupplier, signatureResolver ), UseEvaluation.class );

            FabricReactorHooks.register( internalLogProvider );

            Executor fabricWorkerExecutor = jobScheduler.executor( FABRIC_WORKER );
            var fabricExecutor =
                    new FabricExecutor( fabricConfig, planner, useEvaluation, catalogManager, internalLogProvider, monitoring, fabricWorkerExecutor );
            serviceBootstrapper.registerService( fabricExecutor, FabricExecutor.class );

            serviceBootstrapper.registerService( new TransactionBookmarkManagerFactory( fabricDatabaseManager ), TransactionBookmarkManagerFactory.class );
        }
    }

    public void registerProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        if ( fabricConfig.isEnabled() )
        {
            globalProcedures.register( new GraphIdsFunction( fabricConfig ) );
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
