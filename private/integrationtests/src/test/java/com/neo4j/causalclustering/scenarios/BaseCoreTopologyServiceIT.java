/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public abstract class BaseCoreTopologyServiceIT
{
    private final DiscoveryServiceType discoveryServiceType;
    protected CoreTopologyService service;

    protected BaseCoreTopologyServiceIT( DiscoveryServiceType discoveryServiceType )
    {
        this.discoveryServiceType = discoveryServiceType;
    }

    @BeforeEach
    void setUp()
    {
        // Random members that does not exists, discovery will never succeed
        String initialHosts = "localhost:" + PortAuthority.allocatePort() + ",localhost:" + PortAuthority.allocatePort();
        Config config = Config.defaults();
        config.augment( initial_discovery_members, initialHosts );
        config.augment( CausalClusteringSettings.discovery_listen_address, "localhost:" + PortAuthority.allocatePort() );

        JobScheduler jobScheduler = createInitialisedScheduler();
        InitialDiscoveryMembersResolver initialDiscoveryMemberResolver = new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );

        service = discoveryServiceType
                .createFactory()
                .coreTopologyService( config, new MemberId( UUID.randomUUID() ), jobScheduler, NullLogProvider.getInstance(), NullLogProvider.getInstance(),
                        initialDiscoveryMemberResolver, new NoRetriesStrategy(), new Monitors(), Clocks.systemClock() );
    }

    @Test
    void shouldBeAbleToStartAndStopWithoutSuccessfulJoin()
    {
        assertTimeout( Duration.ofSeconds( 120 ), () ->
        {
            service.init();
            service.start();
            service.stop();
            service.shutdown();
        } );
    }

}
