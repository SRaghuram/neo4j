/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.causalclustering.discovery.RetryStrategy;
import org.neo4j.causalclustering.identity.ClusterBinder;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.DatabaseName;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static java.lang.Thread.sleep;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.CLUSTER_ID;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.DB_NAME;
import static org.neo4j.causalclustering.discovery.ResolutionResolverFactory.chooseResolver;

public class ClusteringModule
{
    private final CoreTopologyService topologyService;
    private final ClusterBinder clusterBinder;

    public ClusteringModule( DiscoveryServiceFactory discoveryServiceFactory, MemberId myself, PlatformModule platformModule,
            CoreStateStorageService coreStateStorage, DatabaseService databaseService )
    {
        LifeSupport life = platformModule.life;
        Config config = platformModule.config;
        LogProvider logProvider = platformModule.logService.getInternalLogProvider();
        LogProvider userLogProvider = platformModule.logService.getUserLogProvider();
        Dependencies dependencies = platformModule.dependencies;
        Monitors monitors = platformModule.monitors;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        RemoteMembersResolver remoteMembersResolver = chooseResolver( config, platformModule.logService );

        topologyService = discoveryServiceFactory.coreTopologyService( config, myself, platformModule.jobScheduler,
                logProvider, userLogProvider, remoteMembersResolver, resolveStrategy( config ), monitors, platformModule.clock );

        life.add( topologyService );

        dependencies.satisfyDependency( topologyService ); // for tests

        CoreBootstrapper coreBootstrapper = new CoreBootstrapper( databaseService, fileSystem, config, logProvider, platformModule.pageCache );

        SimpleStorage<ClusterId> clusterIdStorage;
        SimpleStorage<DatabaseName> dbNameStorage;
        clusterIdStorage = coreStateStorage.simpleStorage( CLUSTER_ID );
        dbNameStorage = coreStateStorage.simpleStorage( DB_NAME );

        String dbName = config.get( CausalClusteringSettings.database );
        int minimumCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );

        Duration clusterBindingTimeout = config.get( CausalClusteringSettings.cluster_binding_timeout );
        clusterBinder = new ClusterBinder( clusterIdStorage, dbNameStorage, topologyService, Clocks.systemClock(), () -> sleep( 100 ),
                clusterBindingTimeout, coreBootstrapper, dbName, minimumCoreHosts, platformModule.monitors );
    }

    private static RetryStrategy resolveStrategy( Config config )
    {
        long refreshPeriodMillis = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        int pollingFrequencyWithinRefreshWindow = 2;
        int numberOfRetries =
                pollingFrequencyWithinRefreshWindow + 1; // we want to have more retries at the given frequency than there is time in a refresh period
        long delayInMillis = refreshPeriodMillis / pollingFrequencyWithinRefreshWindow;
        long retries = numberOfRetries;
        return new RetryStrategy( delayInMillis, retries );
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
