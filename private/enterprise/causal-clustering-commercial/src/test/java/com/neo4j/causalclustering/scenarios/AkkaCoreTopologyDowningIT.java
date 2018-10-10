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
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.causalclustering.discovery.TopologyServiceNoRetriesStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
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
    @Test
    public void shouldReconnectAfterDowning() throws Throwable
    {
        // Given two topology services
        int port1 = PortAuthority.allocatePort();
        int port2 = PortAuthority.allocatePort();

        Pair<AkkaCoreTopologyService,TestActorSystemLifecycle> akkaCoreTopologyService1 = createAndStart( port1, port2 );
        Pair<AkkaCoreTopologyService,TestActorSystemLifecycle> akkaCoreTopologyService2 = createAndStart( port2, port1 );

        // and a working discovery cluster
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 2 );

        // When down one
        akkaCoreTopologyService1.other().down( port2 );

        // Then it is removed
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 1 );

        // And it reappears
        assertEventuallyHasTopologySize( akkaCoreTopologyService1, 2 );
        assertEventuallyHasTopologySize( akkaCoreTopologyService2, 2 );

        // Cleanup
        akkaCoreTopologyService1.first().stop();
        akkaCoreTopologyService2.first().stop();
        akkaCoreTopologyService1.first().shutdown();
        akkaCoreTopologyService2.first().shutdown();
    }

    private void assertEventuallyHasTopologySize( Pair<AkkaCoreTopologyService,TestActorSystemLifecycle> services, int expected ) throws InterruptedException
    {
        Assert.assertEventually( () -> services.first().allCoreServers().members().entrySet(), Matchers.hasSize( expected ), 1, TimeUnit.MINUTES );
    }

    private Pair<AkkaCoreTopologyService,TestActorSystemLifecycle> createAndStart( int myPort, int otherPort ) throws Throwable
    {
        Config config = Config.defaults();
        config.augment( CausalClusteringSettings.discovery_listen_address, "localhost:" + myPort );
        config.augment( CausalClusteringSettings.initial_discovery_members, "localhost:" + myPort + ",localhost:" + otherPort );
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

        RemoteMembersResolver resolver = NoOpHostnameResolver.resolver( config );
        int parallelism = config.get( CausalClusteringSettings.middleware_akka_default_parallelism_level );
        ForkJoinPool pool = new ForkJoinPool( parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true );
        ActorSystemFactory actorSystemFactory = new ActorSystemFactory( resolver, Optional.empty(), pool, config, logProvider  );
        TestActorSystemLifecycle actorSystemLifecycle = new TestActorSystemLifecycle( actorSystemFactory );
        AkkaCoreTopologyService service = new AkkaCoreTopologyService(
                config,
                new MemberId( UUID.randomUUID() ),
                actorSystemLifecycle,
                logProvider,
                logProvider,
                new TopologyServiceNoRetriesStrategy(),
                pool,
                Clocks.systemClock()
        );

        service.init();
        service.start();

        return Pair.of( service, actorSystemLifecycle );
    }

    private static class TestActorSystemLifecycle extends ActorSystemLifecycle
    {
        TestActorSystemLifecycle( ActorSystemFactory actorSystemFactory )
        {
            super( actorSystemFactory, NullLogProvider.getInstance() );
        }

        void down( int port )
        {
            Address address = new Address( "akka", ActorSystemFactory.ACTOR_SYSTEM_NAME, "localhost", port );
            actorSystemComponents.cluster().down( address );
        }
    }
}
