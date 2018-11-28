/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import org.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import org.neo4j.causalclustering.discovery.NoRetriesStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;

import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public abstract class BaseCoreTopologyServiceIT
{
    private final DiscoveryServiceType discoveryServiceType;

    protected BaseCoreTopologyServiceIT( DiscoveryServiceType discoveryServiceType )
    {
        this.discoveryServiceType = discoveryServiceType;
    }

    @Test()
    void shouldBeAbleToStartAndStopWithoutSuccessfulJoin()
    {
        // Random members that does not exists, discovery will never succeed
        String initialHosts = "localhost:" + PortAuthority.allocatePort() + ",localhost:" + PortAuthority.allocatePort();
        Config config = Config.defaults();
        config.augment( initial_discovery_members, initialHosts );
        config.augment( CausalClusteringSettings.discovery_listen_address, "localhost:" + PortAuthority.allocatePort() );

        JobScheduler jobScheduler = createInitialisedScheduler();
        InitialDiscoveryMembersResolver
                initialDiscoveryMemberResolver = new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );

        CoreTopologyService service = discoveryServiceType.createFactory().coreTopologyService(
                config,
                new MemberId( UUID.randomUUID() ),
                jobScheduler,
                NullLogProvider.getInstance(),
                NullLogProvider.getInstance(),
                initialDiscoveryMemberResolver,
                new NoRetriesStrategy(),
                new Monitors(),
                Clocks.systemClock() );

        assertTimeout( Duration.ofSeconds( 120 ), () ->
        {
            service.init();
            service.start();
            service.stop();
            service.shutdown();
        } );
    }

}
