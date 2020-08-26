/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;
import com.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import com.neo4j.server.security.enterprise.EnterpriseSecurityModule;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent;

import java.util.concurrent.Executor;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManager;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.server.security.auth.CommunitySecurityModule;

import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

public interface AbstractEnterpriseEditionModule
{
    /**
     * Satisfy any enterprise only dependencies, that are also needed in other Editions,
     * e.g. {@link CoreEditionModule} and {@link ReadReplicaEditionModule}.
     */
    default void satisfyEnterpriseOnlyDependencies( GlobalModule globalModule )
    {
        // Create Cypher workers
        Config globalConfig = globalModule.getGlobalConfig();
        int configuredWorkers = globalConfig.get( GraphDatabaseInternalSettings.cypher_worker_count );
        // -1 => no Threads
        // 0  => `number of cores` Threads
        // n  => n Threads
        int numberOfThreads =
                configuredWorkers == -1 ? 0 :
                (configuredWorkers == 0 ? Runtime.getRuntime().availableProcessors() :
                 configuredWorkers);
        WorkerManager workerManager =
                new WorkerManager( numberOfThreads, globalModule.getJobScheduler().threadFactory( Group.CYPHER_WORKER ) );
        globalModule.getGlobalDependencies().satisfyDependency( workerManager );
        globalModule.getGlobalLife().add( workerManager );
    }

    private EnterpriseSecurityGraphComponent setupSecurityGraphInitializer( GlobalModule globalModule, SecurityLog securityLog )
    {
        Config config = globalModule.getGlobalConfig();
        FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        LogProvider logProvider = globalModule.getLogService().getUserLogProvider();

        var communityComponent = CommunitySecurityModule.createSecurityComponent( securityLog, config, fileSystem, logProvider );
        var enterpriseComponent = EnterpriseSecurityModule.createSecurityComponent( securityLog, config, fileSystem, logProvider );

        Dependencies dependencies = globalModule.getGlobalDependencies();
        // TODO find a better way to provide a way to let the runtime check the version of the enterprise security graph
        dependencies.satisfyDependency( enterpriseComponent );
        SystemGraphComponents systemGraphComponents = dependencies.resolveDependency( SystemGraphComponents.class );
        systemGraphComponents.register( communityComponent );
        systemGraphComponents.register( enterpriseComponent );

        return enterpriseComponent;
    }

    default SecurityProvider makeEnterpriseSecurityModule( GlobalModule globalModule )
    {
        Config config = globalModule.getGlobalConfig();
        LogProvider userLogProvider = globalModule.getLogService().getUserLogProvider();
        SecurityLog securityLog = new SecurityLog( config, globalModule.getFileSystem() );
        var authCacheExecutor = getAuthCacheExecutor( globalModule );
        CaffeineCacheFactory cacheFactory = new ExecutorBasedCaffeineCacheFactory( authCacheExecutor );
        globalModule.getGlobalLife().add( securityLog );
        EnterpriseSecurityGraphComponent securityComponent = setupSecurityGraphInitializer( globalModule, securityLog );
        if ( config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            EnterpriseSecurityModule securityModule = new EnterpriseSecurityModule(
                    userLogProvider,
                    securityLog, config,
                    globalModule.getGlobalDependencies(),
                    globalModule.getTransactionEventListeners(),
                    securityComponent,
                    cacheFactory,
                    globalModule.getFileSystem()
            );
            securityModule.setup();
            globalModule.getGlobalLife().add( securityModule.authManager() );
            return securityModule;
        }
        return EnterpriseNoAuthSecurityProvider.INSTANCE;
    }

    private Executor getAuthCacheExecutor( GlobalModule globalModule )
    {
        var monitoredJobExecutor = globalModule.getJobScheduler().monitoredJobExecutor( Group.AUTH_CACHE );
        return job -> monitoredJobExecutor.execute( systemJob( "Authentication cache maintenance" ), job );
    }
}
