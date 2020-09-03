/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import akka.actor.Address;
import com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.TestCoreDiscoveryMember;
import com.neo4j.causalclustering.discovery.TestFirstStartupDetector;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.Restarter;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;
import com.neo4j.causalclustering.helper.ErrorHandler;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.EnterpriseDatabaseState;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.internal.helpers.ConstantTimeTimeoutStrategy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.time.Clocks;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.test.assertion.Assert.assertEventually;

// Exercises the case of a downing message reaching a member while it is reachable, which can happen if a partition heals at the right time.
// ClusterShuttingDown will be detected and acted upon.
// Does not trigger the ThisActorSystemQuarantinedEvent. It may not be viable to write a test that does so.
// But much of the code path is the same.
class AkkaCoreTopologyDowningIT
{
    private final List<TopologyServiceComponents> services = new ArrayList<>();
    private final Set<Integer> dynamicPorts = new HashSet<>();

    @AfterEach
    public void afterEach()
    {
        try ( var errorHandler = new ErrorHandler( "Error when trying to shutdown services" ) )
        {
            for ( var service : services )
            {
                errorHandler.execute( () -> stopShutdown( service ) );
            }
            services.clear();
        }
    }

    @Test
    void shouldReconnectAfterDowning() throws Throwable
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
    void shouldRestartIfInitialDiscoMembersNoLongerValid() throws Throwable
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
    void shouldReconnectEachAfterDowningUsingDynamicResolver() throws Throwable
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

    private static void assertEventuallyHasTopologySize( TopologyServiceComponents services, int expected ) throws InterruptedException
    {
        assertEventually( () -> services.topologyService().allCoreServers().entrySet(), new HamcrestCondition<>( Matchers.hasSize( expected ) ),
                5, MINUTES );
    }

    private TopologyServiceComponents createAndStartListResolver( int myPort, int... otherPorts ) throws Throwable
    {
        return createAndStart( NoOpHostnameResolver::resolver, myPort, otherPorts );
    }

    private TopologyServiceComponents createAndStartDynamicResolver( int myPort, int... otherPorts ) throws Throwable
    {
        return createAndStart( config -> new DynamicResolver( dynamicPorts ), myPort, otherPorts );
    }

    private TopologyServiceComponents createAndStart( Function<Config,RemoteMembersResolver> resolverFactory, int myPort, int... otherPorts ) throws Throwable
    {
        var initialDiscoMembers = IntStream.concat( IntStream.of( myPort ), IntStream.of( otherPorts ) )
                .mapToObj( port -> new SocketAddress( "localhost", port ) )
                .collect( Collectors.toList() );

        var boltAddress = new SocketAddress( "localhost", PortAuthority.allocatePort() );

        var config = Config.newBuilder()
                .set( CausalClusteringSettings.discovery_listen_address, new SocketAddress( "localhost", myPort ) )
                .set( CausalClusteringSettings.discovery_advertised_address, new SocketAddress( myPort ) )
                .set( CausalClusteringSettings.initial_discovery_members, initialDiscoMembers )
                .set( BoltConnector.enabled, true )
                .set( BoltConnector.listen_address, boltAddress )
                .set( BoltConnector.advertised_address, boltAddress )
                .set( CausalClusteringSettings.middleware_logging_level, Level.DEBUG )
                .set( GraphDatabaseSettings.store_internal_log_level, Level.DEBUG )
                .build();

        var logProvider = NullLogProvider.getInstance();

        var firstStartupDetector = new TestFirstStartupDetector( true );
        var actorSystemFactory = new ActorSystemFactory( Optional.empty(), firstStartupDetector, config, logProvider  );
        var actorSystemLifecycle = new TestActorSystemLifecycle( actorSystemFactory, resolverFactory, config, logProvider );
        var databaseIdRepository = new TestDatabaseIdRepository();
        Map<NamedDatabaseId,DatabaseState> states = Map.of( databaseIdRepository.defaultDatabase(),
                                                            new EnterpriseDatabaseState( databaseIdRepository.defaultDatabase(), STARTED ) );
        DatabaseStateService databaseStateService = new StubDatabaseStateService( states, EnterpriseDatabaseState::unknown );

        AkkaCoreTopologyService service = new AkkaCoreTopologyService(
                config,
                new InMemoryCoreServerIdentity(),
                actorSystemLifecycle,
                logProvider,
                logProvider,
                new NoRetriesStrategy(),
                new Restarter( new ConstantTimeTimeoutStrategy( 1, MILLISECONDS ), 2 ),
                TestCoreDiscoveryMember::factory,
                createInitialisedScheduler(),
                Clocks.systemClock(),
                new Monitors(),
                databaseStateService
        );

        service.init();
        service.start();

        TopologyServiceComponents pair = new TopologyServiceComponents( service, actorSystemLifecycle );
        services.add( pair );
        return pair;
    }

    private static void stopShutdown( TopologyServiceComponents service ) throws Throwable
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
        private Optional<SocketAddress> firstAddress;

        DynamicResolver( Set<Integer> dynamicPorts )
        {
            this.dynamicPorts = dynamicPorts;
        }

        @Override
        public <COLL extends Collection<REMOTE>, REMOTE> COLL resolve( Function<SocketAddress,REMOTE> transform, Supplier<COLL> collectionFactory )
        {
            SocketAddress[] firstSocketAddressBox = new SocketAddress[1];
            var output = dynamicPorts
                    .stream()
                    .map( port -> new SocketAddress( "localhost", port ) )
                    .sorted( InitialDiscoveryMembersResolver.advertisedSockedAddressComparator )
                    .peek( address -> {
                        if ( firstSocketAddressBox[0] == null )
                        {
                            firstSocketAddressBox[0] = address;
                        }
                    } )
                    .map( transform )
                    .collect( Collectors.toCollection( collectionFactory ) );
            firstAddress = Optional.ofNullable( firstSocketAddressBox[0] );
            return output;
        }

        @Override
        public boolean useOverrides()
        {
            return false;
        }

        @Override
        public Optional<SocketAddress> first()
        {
            return firstAddress;
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
