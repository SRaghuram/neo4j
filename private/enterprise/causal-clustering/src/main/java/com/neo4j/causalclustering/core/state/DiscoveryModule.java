/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryFirstStartupDetector;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.member.DefaultServerSnapshot;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.configuration.CausalClusteringSettings;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerIdentity;
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
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final SslPolicyLoader sslPolicyLoader;
    private final DatabaseStateService databaseStateService;
    private final Panicker panicker;
    private final LifeSupport globalLife;
    private final Config globalConfig;
    private final Dependencies globalDependencies;
    private final Monitors globalMonitors;
    private final LogService logService;

    public DiscoveryModule( DiscoveryServiceFactory discoveryServiceFactory, GlobalModule globalModule,
                            SslPolicyLoader sslPolicyLoader, DatabaseStateService databaseStateService,
                            Panicker panicker )
    {
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.sslPolicyLoader = sslPolicyLoader;
        this.databaseStateService = databaseStateService;
        this.panicker = panicker;
        this.globalLife = globalModule.getGlobalLife();
        this.globalConfig = globalModule.getGlobalConfig();
        this.logService = globalModule.getLogService();
        this.globalDependencies = globalModule.getGlobalDependencies();

        this.clock = globalModule.getGlobalClock();
        this.jobScheduler = globalModule.getJobScheduler();

        this.globalMonitors = globalModule.getGlobalMonitors();

        this.debugLog = logService.getInternalLogProvider();
        this.userLog = logService.getUserLogProvider();
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

    public CoreTopologyService coreTopologyService( CoreServerIdentity identity, DiscoveryFirstStartupDetector firstStartupDetector )
    {
        RemoteMembersResolver remoteMembersResolver = chooseResolver( globalConfig, logService );
        var serverSnapshotFactory = DefaultServerSnapshot.coreSnapshotFactory( identity );
        CoreTopologyService topologyService = discoveryServiceFactory.coreTopologyService( globalConfig, identity, jobScheduler, debugLog, userLog,
                                                                                           remoteMembersResolver, resolveStrategy( globalConfig ),
                                                                                           sslPolicyLoader, serverSnapshotFactory, firstStartupDetector,
                                                                                           globalMonitors, clock,
                                                                                           databaseStateService, panicker );
        globalLife.add( topologyService );
        globalDependencies.satisfyDependency( topologyService ); // for tests
        return topologyService;
    }

    public TopologyService standaloneTopologyService( ServerIdentity identity )
    {
        RemoteMembersResolver remoteMembersResolver = chooseResolver( globalConfig, logService );
        TopologyService topologyService = discoveryServiceFactory.standaloneTopologyService( globalConfig, identity, jobScheduler, debugLog, userLog,
                                                                                             remoteMembersResolver, resolveStrategy( globalConfig ),
                                                                                             sslPolicyLoader, DefaultServerSnapshot::rrSnapshot,
                                                                                             globalMonitors, clock,
                                                                                             databaseStateService, panicker );
        globalLife.add( topologyService );
        globalDependencies.satisfyDependency( topologyService ); // for tests
        return topologyService;
    }
}
