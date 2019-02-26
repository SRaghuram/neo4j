/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import akka.actor.Address;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;
import com.neo4j.causalclustering.identity.MemberId;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.time.Clocks;

import static org.neo4j.test.assertion.Assert.assertEventually;

// Exercises the case of a downing message reaching a member while it is reachable, which can happen if a partition heals at the right time.
// ClusterShuttingDown will be detected and acted upon.
// Does not trigger the ThisActorSystemQuarantinedEvent. It may not be viable to write a test that does so.
// But much of the code path is the same.
public class AkkaCoreTopologyDowningIT
{
    private List<TopologyServiceComponents> services = new ArrayList<>();
    private Set<Integer> dynamicPorts = new HashSet<>();

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
        services.clear();
    }

    @Test
    public void shouldReconnectAfterDowning() throws Throwable
    {
        // Given two topology services
        int port1 = PortAuthority.allocatePort();
        int port2 = PortAuthority.allocatePort();

        TopologyServiceComponents akkaCoreTopologyService1 = createAndStartListResolver( port1, port2 );
        TopologyServiceComponents akkaCoreTopologyService2 = createAndStartListResolver( port2, port1 );

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

        TopologyServiceComponents akkaCoreTopologyService1 = createAndStartListResolver( port1, port2 );
        TopologyServiceComponents akkaCoreTopologyService2 = createAndStartListResolver( port2, port1 );

        // and a working discovery cluster
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 2 );

        // When add 3rd
        int port3 = PortAuthority.allocatePort();
        TopologyServiceComponents akkaCoreTopologyService3 = createAndStartListResolver( port3, port1, port2 );

        // And 3rd connected
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 3 );

        // And shutdown 2nd
        stopShutdown( akkaCoreTopologyService2 );
        services.remove( akkaCoreTopologyService2 );

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

    @Test
    public void shouldReconnectEachAfterDowningUsingDynamicResolver() throws Throwable
    {
        // Given two topology services
        int port1 = PortAuthority.allocatePort();
        int port2 = PortAuthority.allocatePort();
        int port3 = PortAuthority.allocatePort();

        dynamicPorts.add( port1 );
        dynamicPorts.add( port2 );
        dynamicPorts.add( port3 );

        TopologyServiceComponents akkaCoreTopologyService1 = createAndStartDynamicResolver( port1 );
        TopologyServiceComponents akkaCoreTopologyService2 = createAndStartDynamicResolver( port2 );
        TopologyServiceComponents akkaCoreTopologyService3 = createAndStartDynamicResolver( port3 );

        // and a working discovery cluster
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 3 );

        // When down first
        akkaCoreTopologyService2.actorSystemLifecycle().down( port1 );

        // Then it is removed
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 2 );

        // And it reappears
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 3 );

        // When down second
        akkaCoreTopologyService1.actorSystemLifecycle().down( port2 );

        // Then it is removed
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 2 );

        // And it reappears
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 3 );

        // When down third
        akkaCoreTopologyService2.actorSystemLifecycle().down( port3 );

        // Then it is removed
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 2 );

        // And it reappears
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 3 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService3, 3 );
    }

    private void assertEventuallyHasTopologySize( TopologyServiceComponents services, int expected ) throws InterruptedException
    {
        assertEventually( () -> services.topologyService().allCoreServers().members().entrySet(), Matchers.hasSize( expected ), 5, TimeUnit.MINUTES );
    }

    private TopologyServiceComponents createAndStartListResolver( int myPort, int... otherPorts ) throws Throwable
    {
        return createAndStart( NoOpHostnameResolver::resolver, myPort, otherPorts );
    }

    private TopologyServiceComponents createAndStartDynamicResolver( int myPort, int... otherPorts ) throws Throwable
    {
        return createAndStart( config -> new DynamicResolver( dynamicPorts, config ), myPort, otherPorts );
    }

    private TopologyServiceComponents createAndStart( Function<Config,RemoteMembersResolver> resolverFactory, int myPort, int... otherPorts ) throws Throwable
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
        TestActorSystemLifecycle actorSystemLifecycle = new TestActorSystemLifecycle( actorSystemFactory, resolverFactory, config, logProvider );
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
    }

    private static class TestActorSystemLifecycle extends ActorSystemLifecycle
    {
        TestActorSystemLifecycle( ActorSystemFactory actorSystemFactory, Function<Config,RemoteMembersResolver> resolverFactory,
                Config config, LogProvider logProvider )
        {
            this( actorSystemFactory, resolverFactory.apply( config ), config, logProvider );
        }

        private TestActorSystemLifecycle( ActorSystemFactory actorSystemFactory, RemoteMembersResolver resolver, Config config, LogProvider logProvider )
        {
            super( actorSystemFactory, resolver, new JoinMessageFactory( resolver ), config, logProvider );
        }

        void down( int port )
        {
            Address address = new Address( "akka", ActorSystemFactory.ACTOR_SYSTEM_NAME, "localhost", port );
            actorSystemComponents.cluster().down( address );
        }
    }

    private static class DynamicResolver implements RemoteMembersResolver
    {
        private final Set<Integer> dynamicPorts;

        DynamicResolver( Set<Integer> dynamicPorts, Config config )
        {
            this.dynamicPorts = dynamicPorts;
        }

        @Override
        public <COLL extends Collection<REMOTE>, REMOTE> COLL resolve( Function<AdvertisedSocketAddress,REMOTE> transform, Supplier<COLL> collectionFactory )
        {
            return dynamicPorts
                    .stream()
                    .map( port -> new AdvertisedSocketAddress( "localhost", port ) )
                    .sorted( InitialDiscoveryMembersResolver.advertisedSockedAddressComparator )
                    .map( transform )
                    .collect( Collectors.toCollection( collectionFactory ) );
        }

        @Override
        public boolean useOverrides()
        {
            return false;
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
