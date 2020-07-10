/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.configuration.CausalClusteringSettings;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static com.neo4j.configuration.ResolutionResolverFactory.chooseResolver;

public class DiscoveryModule
{
    private final SystemNanoClock clock;
    private final JobScheduler jobScheduler;
    private final LogProvider userLog;
    private final LogProvider debugLog;
    private final CoreTopologyService topologyService;

    public DiscoveryModule( ClusteringIdentityModule identityModule, DiscoveryServiceFactory discoveryServiceFactory,
            DiscoveryMemberFactory discoveryMemberFactory, GlobalModule globalModule, SslPolicyLoader sslPolicyLoader )
    {
        LifeSupport globalLife = globalModule.getGlobalLife();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        Dependencies globalDependencies = globalModule.getGlobalDependencies();

        this.clock = globalModule.getGlobalClock();
        this.jobScheduler = globalModule.getJobScheduler();

        Monitors globalMonitors = globalModule.getGlobalMonitors();

        this.debugLog = logService.getInternalLogProvider();
        this.userLog = logService.getUserLogProvider();

        this.topologyService = createDiscoveryService( identityModule, discoveryServiceFactory, discoveryMemberFactory, sslPolicyLoader,
                globalLife, globalConfig, logService, globalMonitors, globalDependencies );
    }

    private CoreTopologyService createDiscoveryService( ClusteringIdentityModule identityModule, DiscoveryServiceFactory discoveryServiceFactory,
            DiscoveryMemberFactory discoveryMemberFactory, SslPolicyLoader sslPolicyLoader, LifeSupport life, Config config,
            LogService logService, Monitors monitors, Dependencies dependencies )
    {
        RemoteMembersResolver remoteMembersResolver = chooseResolver( config, logService );
        CoreTopologyService topologyService = discoveryServiceFactory.coreTopologyService( config, identityModule, jobScheduler, debugLog, userLog,
                remoteMembersResolver, resolveStrategy( config ), sslPolicyLoader, discoveryMemberFactory, monitors, clock );
        life.add( topologyService );
        dependencies.satisfyDependency( topologyService ); // for tests
        return topologyService;
    }

    private static RetryStrategy resolveStrategy( Config config )
    {
        long refreshPeriodMillis = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        int pollingFrequencyWithinRefreshWindow = 2;
        // we want to have more retries at the given frequency than there is time in a refresh period
        int numberOfRetries = pollingFrequencyWithinRefreshWindow + 1;
        long delayInMillis = refreshPeriodMillis / pollingFrequencyWithinRefreshWindow;
        return new RetryStrategy( delayInMillis, numberOfRetries );
    }

    public CoreTopologyService topologyService()
    {
        return topologyService;
    }
}
