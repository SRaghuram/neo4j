/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.TestDiscoveryMember;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

class AkkaCoreTopologyServiceIT
{
    private AkkaCoreTopologyService service;
    private JobScheduler jobScheduler;

    @BeforeEach
    void setUp()
    {
        // Random members that does not exists, discovery will never succeed
        var initialHosts = "localhost:" + PortAuthority.allocatePort() + ",localhost:" + PortAuthority.allocatePort();
        var config = Config.defaults();
        config.augment( initial_discovery_members, initialHosts );
        config.augment( CausalClusteringSettings.discovery_listen_address, "localhost:" + PortAuthority.allocatePort() );

        var discoveryMember = new TestDiscoveryMember();

        var logProvider = NullLogProvider.getInstance();
        jobScheduler = createInitialisedScheduler();
        var initialDiscoveryMemberResolver = new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );
        var sslPolicyLoader = SslPolicyLoader.create( config, logProvider );

        service = new AkkaDiscoveryServiceFactory().coreTopologyService( config, discoveryMember, jobScheduler, logProvider, logProvider,
                initialDiscoveryMemberResolver, new NoRetriesStrategy(), sslPolicyLoader, new Monitors(), Clocks.systemClock() );
    }

    @AfterEach
    void cleanUp() throws Exception
    {
        jobScheduler.shutdown();
    }

    @Test
    void shouldRestart() throws Throwable
    {
        service.init();
        service.start();
        service.restart();
        service.stop();
        service.shutdown();
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
