/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.TestDiscoveryMember;
import com.neo4j.causalclustering.discovery.TestFirstStartupDetector;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.Clocks;

import static com.neo4j.configuration.CausalClusteringSettings.initial_discovery_members;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.test.ports.PortAuthority.allocatePort;

class AkkaCoreTopologyServiceIT
{
    private AkkaCoreTopologyService service;
    private JobScheduler jobScheduler;

    @BeforeEach
    void setUp()
    {
        // Random members that does not exists, discovery will never succeed
        var initialHosts = List.of( new SocketAddress( "localhost", allocatePort() ), new SocketAddress( "localhost", allocatePort() ) );
        var config = Config.newBuilder()
                .set( initial_discovery_members, initialHosts )
                .set( CausalClusteringSettings.discovery_listen_address, new SocketAddress( "localhost", allocatePort() ) )
                .build();

        var identityModule = new StubClusteringIdentityModule();

        var logProvider = NullLogProvider.getInstance();
        jobScheduler = createInitialisedScheduler();
        var initialDiscoveryMemberResolver = new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );
        var sslPolicyLoader = SslPolicyLoader.create( config, logProvider );
        var firstStartupDetector = new TestFirstStartupDetector( true );
        var databaseStateService = new StubDatabaseStateService( dbId -> new EnterpriseDatabaseState( dbId, EnterpriseOperatorState.STARTED ) );

        service = new AkkaDiscoveryServiceFactory().coreTopologyService( config, identityModule, jobScheduler, logProvider, logProvider,
                                                                         initialDiscoveryMemberResolver, new NoRetriesStrategy(), sslPolicyLoader,
                                                                         TestDiscoveryMember::factory, firstStartupDetector,
                                                                         new Monitors(), Clocks.systemClock(), databaseStateService );
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
        assertTimeoutPreemptively( Duration.ofSeconds( 120 ), () ->
        {
            service.init();
            service.start();
            service.stop();
            service.shutdown();
        } );
    }

}
