/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bootstrap;

import com.neo4j.fabric.auth.CredentialsProvider;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.eval.Catalog;
import com.neo4j.fabric.eval.FromEvaluation;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.executor.FabricLocalExecutor;
import com.neo4j.fabric.executor.FabricRemoteExecutor;
import com.neo4j.fabric.functions.GraphIdsFunction;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.pipeline.SignatureResolver;
import com.neo4j.fabric.planning.FabricPlanner;
import com.neo4j.fabric.transaction.TransactionManager;

import java.time.Clock;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.CypherConfiguration;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

public class FabricServicesBootstrap
{

    private final FabricConfig fabricConfig;

    public FabricServicesBootstrap( LifeSupport lifeSupport, Dependencies dependencies )
    {
        var serviceBootstrapper = new ServiceBootstrapper( lifeSupport, dependencies );
        var config = dependencies.resolveDependency( Config.class );
        fabricConfig = serviceBootstrapper.registerService( FabricConfig.from( config ), FabricConfig.class );
        var fabricDatabaseManager = serviceBootstrapper.registerService(
                new FabricDatabaseManager( fabricConfig, dependencies ),
                FabricDatabaseManager.class );

        if ( fabricConfig.isEnabled() )
        {
            var jobScheduler = dependencies.resolveDependency( JobScheduler.class );
            var credentialsProvider = serviceBootstrapper.registerService( new CredentialsProvider(), CredentialsProvider.class );
            var driverPool = serviceBootstrapper
                    .registerService( new DriverPool( jobScheduler, fabricConfig, config, Clock.systemUTC(), credentialsProvider ), DriverPool.class );
            serviceBootstrapper.registerService( new FabricRemoteExecutor( driverPool ), FabricRemoteExecutor.class );
            serviceBootstrapper.registerService( new FabricLocalExecutor( fabricConfig, fabricDatabaseManager ), FabricLocalExecutor.class );
            serviceBootstrapper.registerService( new TransactionManager( dependencies ), TransactionManager.class );
            var monitors = new Monitors();
            var cypherConfig = CypherConfiguration.fromConfig( config );
            Supplier<GlobalProcedures> proceduresSupplier = () -> dependencies.resolveDependency( GlobalProcedures.class );
            var signatureResolver = new SignatureResolver( proceduresSupplier );
            var planner = serviceBootstrapper.registerService( new FabricPlanner( fabricConfig, cypherConfig, monitors, signatureResolver ), FabricPlanner.class );
            var catalog = Catalog.fromConfig( fabricConfig );
            var fromEvaluation = serviceBootstrapper.registerService( new FromEvaluation( catalog, proceduresSupplier, signatureResolver), FromEvaluation.class );
            var executor = new FabricExecutor( fabricConfig, planner, fromEvaluation );
            serviceBootstrapper.registerService( executor, FabricExecutor.class );
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
