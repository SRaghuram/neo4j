/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.TestFirstStartupDetector;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.ActorSystemRestarter;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.DummyPanicService;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;
import com.neo4j.causalclustering.discovery.member.TestCoreDiscoveryMember;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.EnterpriseDatabaseState;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.Level;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.time.Clocks;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.test.assertion.Assert.assertEventually;

class AkkaCoreTopologyMisConfiguredIT
{
    @Test
    void mixedCaseAkkaHostnamesShouldWork() throws Throwable
    {
        var port1 = PortAuthority.allocatePort();
        var port2 = PortAuthority.allocatePort();
        var port3 = PortAuthority.allocatePort();
        var akkaCoreTopologyService1 = createAndStart( port1, port1, port2, port3 );
        var akkaCoreTopologyService2 = createAndStart( port2, port1, port2, port3 );
        var akkaCoreTopologyService3 = createAndStart( port3, port1, port2, port3 );

        assertEventuallyHasTopologySize( akkaCoreTopologyService1 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3 );
        stopShutdown( akkaCoreTopologyService1 );
        stopShutdown( akkaCoreTopologyService2 );
        stopShutdown( akkaCoreTopologyService3 );
    }

    private static AkkaCoreTopologyService createAndStart( int myPort, int... otherPorts ) throws Exception
    {
        var initialDiscoMembers = IntStream.concat( IntStream.of( myPort ), IntStream.of( otherPorts ) )
                .mapToObj( port -> new SocketAddress( "LOCALHOST", port ) )
                .collect( Collectors.toList() );

        var config = Config.newBuilder()
                .set( CausalClusteringSettings.discovery_listen_address, new SocketAddress( "localHost", myPort ) )
                .set( CausalClusteringSettings.discovery_advertised_address, new SocketAddress( "LocalHost", myPort ) )
                .set( CausalClusteringSettings.initial_discovery_members, initialDiscoMembers )
                .set( CausalClusteringSettings.middleware_logging_level, Level.DEBUG )
                .set( GraphDatabaseSettings.store_internal_log_level, Level.DEBUG )
                .build();

        var logProvider = NullLogProvider.getInstance();
        var jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        var firstStartupDetector = new TestFirstStartupDetector( true );
        var actorSystemFactory = new ActorSystemFactory( Optional.empty(), firstStartupDetector, config, logProvider  );
        var resolver = NoOpHostnameResolver.resolver( config );
        var actorSystemLifecycle = new ActorSystemLifecycle( actorSystemFactory, resolver, new JoinMessageFactory( resolver ), config, logProvider );
        var databaseIdRepository = new TestDatabaseIdRepository();
        var panicker = DummyPanicService.PANICKER;
        Map<NamedDatabaseId,DatabaseState> states = Map.of( databaseIdRepository.defaultDatabase(),
                new EnterpriseDatabaseState( databaseIdRepository.defaultDatabase(), STARTED ) );
        var databaseStateService = new StubDatabaseStateService( states, EnterpriseDatabaseState::unknown );
        var service = new AkkaCoreTopologyService(
                config,
                new InMemoryCoreServerIdentity(),
                actorSystemLifecycle,
                logProvider,
                logProvider,
                new NoRetriesStrategy(),
                ActorSystemRestarter.forTest( 2 ),
                TestCoreDiscoveryMember::new,
                jobScheduler,
                Clocks.systemClock(),
                new Monitors(),
                databaseStateService,
                panicker
        );

        service.init();
        service.start();
        return service;
    }

    private static void assertEventuallyHasTopologySize( TopologyService service )
    {
        assertEventually( () -> service.allCoreServers().entrySet(), new HamcrestCondition<>( Matchers.hasSize( 3 ) ), 2, MINUTES );
    }

    private static void stopShutdown( TopologyService service ) throws Exception
    {
        service.stop();
        service.shutdown();
    }
}
