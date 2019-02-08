/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.helper.TemporaryDatabase;
import com.neo4j.causalclustering.identity.ClusterBinder;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.DatabaseName;
import com.neo4j.causalclustering.identity.MemberId;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.CLUSTER_ID;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.DB_NAME;
import static com.neo4j.causalclustering.discovery.ResolutionResolverFactory.chooseResolver;
import static java.lang.Thread.sleep;

public class ClusteringModule
{
    private final CoreTopologyService topologyService;
    private final ClusterBinder clusterBinder;

    public ClusteringModule( DiscoveryServiceFactory discoveryServiceFactory, MemberId myself, GlobalModule globalModule,
            CoreStateStorageService coreStateStorage, DatabaseService databaseService, Function<String,DatabaseInitializer> databaseInitializers )
    {
        LifeSupport globalLife = globalModule.getGlobalLife();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        LogProvider logProvider = logService.getInternalLogProvider();
        LogProvider userLogProvider = logService.getUserLogProvider();
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Monitors globalMonitors = globalModule.getGlobalMonitors();
        FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        RemoteMembersResolver remoteMembersResolver = chooseResolver( globalConfig, logService );

        topologyService = discoveryServiceFactory.coreTopologyService( globalConfig, myself, globalModule.getJobScheduler(),
                logProvider, userLogProvider, remoteMembersResolver, resolveStrategy( globalConfig ), globalMonitors, globalModule.getGlobalClock() );

        globalLife.add( topologyService );

        globalDependencies.satisfyDependency( topologyService ); // for tests

        PageCache pageCache = globalModule.getPageCache();
        TemporaryDatabase.Factory tempDatabaseFactory = new TemporaryDatabase.Factory( pageCache );

        CoreBootstrapper coreBootstrapper = new CoreBootstrapper( databaseService, tempDatabaseFactory, databaseInitializers, fileSystem,
                globalConfig, logProvider, pageCache, globalModule.getStorageEngineFactory() );

        SimpleStorage<ClusterId> clusterIdStorage;
        SimpleStorage<DatabaseName> dbNameStorage;
        clusterIdStorage = coreStateStorage.simpleStorage( CLUSTER_ID );
        dbNameStorage = coreStateStorage.simpleStorage( DB_NAME );

        String dbName = globalConfig.get( CausalClusteringSettings.database );
        int minimumCoreHosts = globalConfig.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );

        Duration clusterBindingTimeout = globalConfig.get( CausalClusteringSettings.cluster_binding_timeout );
        clusterBinder = new ClusterBinder( clusterIdStorage, dbNameStorage, topologyService, Clocks.systemClock(), () -> sleep( 100 ),
                clusterBindingTimeout, coreBootstrapper, dbName, minimumCoreHosts, globalMonitors );
    }

    private static RetryStrategy resolveStrategy( Config config )
    {
        long refreshPeriodMillis = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        int pollingFrequencyWithinRefreshWindow = 2;
        int numberOfRetries =
                pollingFrequencyWithinRefreshWindow + 1; // we want to have more retries at the given frequency than there is time in a refresh period
        long delayInMillis = refreshPeriodMillis / pollingFrequencyWithinRefreshWindow;
        return new RetryStrategy( delayInMillis, numberOfRetries );
    }

    public CoreTopologyService topologyService()
    {
        return topologyService;
    }

    public Supplier<Optional<ClusterId>> clusterIdentity()
    {
        return clusterBinder;
    }

    public ClusterBinder clusterBinder()
    {
        return clusterBinder;
    }
}
