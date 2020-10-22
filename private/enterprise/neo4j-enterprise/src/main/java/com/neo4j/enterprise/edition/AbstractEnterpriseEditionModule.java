/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;
import com.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import com.neo4j.server.security.enterprise.EnterpriseSecurityModule;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseDefaultDatabaseResolver;
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManager;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.server.security.auth.CommunitySecurityModule;

import static org.neo4j.scheduler.JobMonitoringParams.systemJob;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

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

    default void registerSecurityComponents( SystemGraphComponents systemGraphComponents, GlobalModule globalModule, SecurityLog securityLog )
    {
        Config config = globalModule.getGlobalConfig();
        FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        LogProvider logProvider = globalModule.getLogService().getUserLogProvider();

        var userSecurityComponent = CommunitySecurityModule.createSecurityComponent( securityLog, config, fileSystem, logProvider );
        var roleAndPrivilegeComponent = EnterpriseSecurityModule.createSecurityComponent( securityLog, config, fileSystem, logProvider );
        systemGraphComponents.register( userSecurityComponent );
        systemGraphComponents.register( roleAndPrivilegeComponent );

        Dependencies dependencies = globalModule.getGlobalDependencies();
        dependencies.satisfyDependency( roleAndPrivilegeComponent );
    }

    default SecurityProvider makeEnterpriseSecurityModule( GlobalModule globalModule, DefaultDatabaseResolver defaultDatabaseResolver, SecurityLog securityLog )
    {
        var config = globalModule.getGlobalConfig();
        var userLogProvider = globalModule.getLogService().getUserLogProvider();
        var dependencies = globalModule.getGlobalDependencies();
        var authCacheExecutor = getAuthCacheExecutor( globalModule );
        var cacheFactory = new ExecutorBasedCaffeineCacheFactory( authCacheExecutor );
        if ( config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            var securityComponent = dependencies.resolveDependency( EnterpriseSecurityGraphComponent.class );
            EnterpriseSecurityModule securityModule = new EnterpriseSecurityModule(
                    userLogProvider,
                    securityLog, config,
                    dependencies,
                    globalModule.getTransactionEventListeners(),
                    securityComponent,
                    cacheFactory,
                    globalModule.getFileSystem(),
                    defaultDatabaseResolver
            );
            securityModule.setup();
            globalModule.getGlobalLife().add( securityModule.authManager() );
            AuthManager loopbackAuthManager = securityModule.loopbackAuthManager();
            if ( loopbackAuthManager != null )
            {
                globalModule.getGlobalLife().add( loopbackAuthManager );
            }
            return securityModule;
        }
        return EnterpriseNoAuthSecurityProvider.INSTANCE;
    }

    private Supplier<GraphDatabaseService> systemSupplier( DependencyResolver dependencies )
    {
        return () ->
        {
            DatabaseManager<?> databaseManager = dependencies.resolveDependency( DatabaseManager.class );
            return databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                    () -> new RuntimeException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
        };
    }

    default EnterpriseDefaultDatabaseResolver makeDefaultDatabaseResolver( GlobalModule globalModule )
    {
        var authCacheExecutor = getAuthCacheExecutor( globalModule );
        CaffeineCacheFactory cacheFactory = new ExecutorBasedCaffeineCacheFactory( authCacheExecutor );
        return new EnterpriseDefaultDatabaseResolver( globalModule.getGlobalConfig(), cacheFactory, systemSupplier( globalModule.getGlobalDependencies() ) );
    }

    private Executor getAuthCacheExecutor( GlobalModule globalModule )
    {
        var monitoredJobExecutor = globalModule.getJobScheduler().monitoredJobExecutor( Group.AUTH_CACHE );
        return job -> monitoredJobExecutor.execute( systemJob( "Authentication cache maintenance" ), job );
    }
}
