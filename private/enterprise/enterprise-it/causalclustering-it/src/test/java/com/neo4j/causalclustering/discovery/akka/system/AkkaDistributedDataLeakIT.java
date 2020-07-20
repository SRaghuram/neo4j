/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.ProviderSelection;
import akka.cluster.UniqueAddress;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.cluster.ddata.Replicator;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.AkkaUncleanShutdownDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForServerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Level;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.ports.PortAuthority;

import static com.neo4j.causalclustering.discovery.akka.system.ClusterJoiningActor.AKKA_SCHEME;
import static com.neo4j.configuration.CausalClusteringSettings.discovery_advertised_address;
import static com.neo4j.configuration.CausalClusteringSettings.discovery_listen_address;
import static com.neo4j.configuration.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.configuration.CausalClusteringSettings.middleware_logging_level;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_level;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
class AkkaDistributedDataLeakIT
{
    private static final int TIMEOUT = 60;

    /** Part of Akka cluster. Bootstraps cluster. Listens to changes in distributed data, exposes for assertions */
    private Harness harness;
    /** Will be started/stopped during test. Metadata from this should be cleaned up in distributed data by repairer */
    private CoreTopologyService cleanRestarter;
    /** Will be started/stopped during test. Does not cleanly leave, needs downing from repairer. Should be cleaned up by repairer */
    private CoreTopologyService uncleanRestarter;
    /** Should clean up for other members when they leave the cluster.*/
    private CoreTopologyService repairer;

    private final int metadataCount = 2;

    @BeforeEach
    void setUp() throws Throwable
    {
        harness = new Harness();
        var cleanDiscoveryServiceFactory = new AkkaDiscoveryServiceFactory();
        repairer = AkkaDiscoverySystemHelper.coreTopologyService( harness.port, cleanDiscoveryServiceFactory );
        cleanRestarter = AkkaDiscoverySystemHelper.coreTopologyService( harness.port, cleanDiscoveryServiceFactory );
        uncleanRestarter = AkkaDiscoverySystemHelper.coreTopologyService( harness.port, new AkkaUncleanShutdownDiscoveryServiceFactory() );

        repairer.init();
        cleanRestarter.init();
        uncleanRestarter.init();

        repairer.start();
    }

    @AfterEach
    void tearDown() throws Throwable
    {
        harness.shutdown();
        repairer.stop();
        repairer.shutdown();
        cleanRestarter.shutdown();
        uncleanRestarter.shutdown();
    }

    // Needs hundreds of reps to have a good chance of replicating a leak
    @RepeatedTest( 10 )
    void shouldNotLeakMetadataOnCleanLeave() throws Throwable
    {
        cleanRestarter.start();

        assertEventually( () -> harness.replicatedData.size(), equalityCondition( metadataCount ), TIMEOUT, SECONDS );

        cleanRestarter.stop();

        assertEventually( () -> harness.replicatedData.size(), equalityCondition( metadataCount - 1 ), TIMEOUT, SECONDS );
    }

    @RepeatedTest( 10 )
    void shouldNotLeakMetadataOnUncleanLeave() throws Throwable
    {
        uncleanRestarter.start();

        assertEventually( () -> harness.replicatedData.size(), equalityCondition( metadataCount ), TIMEOUT, SECONDS );

        uncleanRestarter.stop();

        assertEventually( () -> harness.replicatedData.size(), equalityCondition( metadataCount - 1 ), TIMEOUT, SECONDS );
    }

    private static class Harness
    {
        final int port = PortAuthority.allocatePort();
        static final Key<LWWMap<UniqueAddress,CoreServerInfoForServerId>> KEY = LWWMapKey.create( "member-data" );
        LWWMap<?,?> replicatedData = LWWMap.empty();
        private final ActorSystemComponents actorSystemComponents;

        Harness()
        {
            var discoverySocket = new SocketAddress( "localhost", port );
            var config = Config.newBuilder()
                    .set( discovery_listen_address, discoverySocket )
                    .set( discovery_advertised_address, discoverySocket )
                    .set( initial_discovery_members, singletonList( discoverySocket ) )
                    .set( store_internal_log_level, Level.DEBUG )
                    .set( middleware_logging_level, Level.DEBUG )
                    .build();

            var logProvider = NullLogProvider.getInstance();
            var actorSystemFactory = new ActorSystemFactory( Optional.empty(), config, logProvider );
            actorSystemComponents = new ActorSystemComponents( actorSystemFactory, ProviderSelection.cluster() );
            actorSystemComponents.cluster().joinSeedNodes(
                    singletonList( new Address( AKKA_SCHEME, actorSystemComponents.cluster().system().name(), "localhost", port ) ) );

            var listener = actorSystemComponents.actorSystem().actorOf( ReplicatorListener.props( this ) );

            actorSystemComponents.replicator().tell( new Replicator.Subscribe<>( KEY, listener ), ActorRef.noSender() );
        }

        public void shutdown()
        {
            actorSystemComponents.coordinatedShutdown().runAll();
        }
    }

    private static class ReplicatorListener extends AbstractActor
    {
        private final Harness harness;

        public static Props props( Harness harness )
        {
            return Props.create( ReplicatorListener.class, () -> new ReplicatorListener( harness ) );
        }

        ReplicatorListener( Harness harness )
        {
            this.harness = harness;
        }

        @Override
        public Receive createReceive()
        {
            return ReceiveBuilder.create()
                    .match(  Replicator.Changed.class,
                            msg -> msg.key().equals( Harness.KEY ),
                            message -> harness.replicatedData = (LWWMap<?,?>) message.dataValue() )
                    .build();
        }
    }
}
