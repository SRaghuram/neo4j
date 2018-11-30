/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import akka.actor.Address;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import org.neo4j.causalclustering.discovery.NoRetriesStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.assertion.Assert;
import org.neo4j.time.Clocks;

// Exercises the case of a downing message reaching a member while it is reachable, which can happen if a partition heals at the right time.
// ClusterShuttingDown will be detected and acted upon.
// Does not trigger the ThisActorSystemQuarantinedEvent. It may not be viable to write a test that does so.
// But much of the code path is the same.
public class AkkaCoreTopologyDowningIT
{
    private List<TopologyServiceComponents> services = new ArrayList<>();

    @After
    public void tearDown()
    {
        for ( TopologyServiceComponents service : services )
        {
            try
            {
                stopShutdown( service );
            }
            catch ( Throwable throwable )
            {
                throwable.printStackTrace();
            }
        }
    }

    @Test
    public void shouldReconnectAfterDowning() throws Throwable
    {
        // Given two topology services
        int port1 = PortAuthority.allocatePort();
        int port2 = PortAuthority.allocatePort();

        TopologyServiceComponents akkaCoreTopologyService1 = createAndStart( port1, port2 );
        TopologyServiceComponents akkaCoreTopologyService2 = createAndStart( port2, port1 );

        // and a working discovery cluster
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 2 );

        // When down one
        akkaCoreTopologyService1.actorSystemLifecycle().down( port2 );

        // Then it is removed
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 1 );

        // And it reappears
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 2 );
    }

    @Test
    public void shouldRestartIfInitialDiscoMembersNoLongerValid() throws Throwable
    {
        // Given two topology services
        int port1 = PortAuthority.allocatePort();
        int port2 = PortAuthority.allocatePort();

        TopologyServiceComponents akkaCoreTopologyService1 = createAndStart( port1, port2 );
        TopologyServiceComponents akkaCoreTopologyService2 = createAndStart( port2, port1 );

        // and a working discovery cluster
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 2 );

        // When add 3rd
        int port3 = PortAuthority.allocatePort();
        TopologyServiceComponents akkaCoreTopologyService3 = createAndStart( port3, port1, port2 );

        // And 3rd connected
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 3 );

        // And shutdown 2nd
        stopShutdown( akkaCoreTopologyService2 );

        // And 2nd's gone
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 2 );

        // And down 1st from 3rd
        akkaCoreTopologyService3.actorSystemLifecycle().down( port1 );

        // Then 1st is removed
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 1 );

        // And 1st reappears
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 2 );
    }

    private void assertEventuallyHasTopologySize( TopologyServiceComponents services, int expected ) throws InterruptedException
    {
        Assert.assertEventually( () -> services.topologyService().allCoreServers().members().entrySet(), Matchers.hasSize( expected ), 5, TimeUnit.MINUTES );
    }

    private TopologyServiceComponents createAndStart( int myPort, int... otherPorts ) throws Throwable
    {
        Config config = Config.defaults();
        config.augment( CausalClusteringSettings.discovery_listen_address, "localhost:" + myPort );
        String initialDiscoMembers = IntStream.concat( IntStream.of( myPort ), IntStream.of( otherPorts ) )
                .mapToObj( port -> "localhost:" + port )
                .collect( Collectors.joining( "," ) );
        config.augment( CausalClusteringSettings.initial_discovery_members, initialDiscoMembers );
        BoltConnector boltConnector = new BoltConnector( "bolt" );
        AdvertisedSocketAddress boltAddress = new AdvertisedSocketAddress( "localhost", PortAuthority.allocatePort() );
        config.augment( boltConnector.type.name(), "BOLT" );
        config.augment( boltConnector.enabled.name(), "true" );
        config.augment( boltConnector.listen_address.name(), boltAddress.toString() );
        config.augment( boltConnector.advertised_address.name(), boltAddress.toString() );

        config.augment( CausalClusteringSettings.middleware_logging_level, "0" );
        config.augment( CausalClusteringSettings.disable_middleware_logging, "false" );
        config.augment( "dbms.logs.debug.level", "DEBUG" );
        LogProvider logProvider = NullLogProvider.getInstance();

        int parallelism = config.get( CausalClusteringSettings.middleware_akka_default_parallelism_level );
        ForkJoinPool pool = new ForkJoinPool( parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true );
        ActorSystemFactory actorSystemFactory = new ActorSystemFactory( Optional.empty(), pool, config, logProvider  );
        TestActorSystemLifecycle actorSystemLifecycle = new TestActorSystemLifecycle( actorSystemFactory, config, logProvider );
        AkkaCoreTopologyService service = new AkkaCoreTopologyService(
                config,
                new MemberId( UUID.randomUUID() ),
                actorSystemLifecycle,
                logProvider,
                logProvider,
                new NoRetriesStrategy(),
                pool,
                Clocks.systemClock()
        );

        service.init();
        service.start();

        TopologyServiceComponents pair = new TopologyServiceComponents( service, actorSystemLifecycle );
        services.add( pair );
        return pair;
    }

    private void stopShutdown( TopologyServiceComponents service ) throws Throwable
    {
        service.topologyService().stop();
        service.topologyService().shutdown();
        services.remove( service );
    }

    private static class TestActorSystemLifecycle extends ActorSystemLifecycle
    {
        TestActorSystemLifecycle( ActorSystemFactory actorSystemFactory, Config config, LogProvider logProvider )
        {
            super( actorSystemFactory, NoOpHostnameResolver.resolver( config ), config, logProvider );
        }

        void down( int port )
        {
            Address address = new Address( "akka", ActorSystemFactory.ACTOR_SYSTEM_NAME, "localhost", port );
            actorSystemComponents.cluster().down( address );
        }
    }

    private static class TopologyServiceComponents
    {
        private final AkkaCoreTopologyService topologyService;
        private final TestActorSystemLifecycle actorSystemLifecycle;

        private TopologyServiceComponents( AkkaCoreTopologyService topologyService, TestActorSystemLifecycle actorSystemLifecycle )
        {
            this.topologyService = topologyService;
            this.actorSystemLifecycle = actorSystemLifecycle;
        }

        AkkaCoreTopologyService topologyService()
        {
            return topologyService;
        }

        TestActorSystemLifecycle actorSystemLifecycle()
        {
            return actorSystemLifecycle;
        }
    }
}
