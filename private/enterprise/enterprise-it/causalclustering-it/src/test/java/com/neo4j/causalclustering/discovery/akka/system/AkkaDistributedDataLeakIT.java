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
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TestDiscoveryMember;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.Level;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_advertised_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_listen_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_logging_level;
import static com.neo4j.causalclustering.discovery.akka.system.ClusterJoiningActor.AKKA_SCHEME;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.SERVER_DEFAULTS;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_level;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
class AkkaDistributedDataLeakIT
{
    private static final int TIMEOUT = 20;
    /** Part of Akka cluster. Bootstraps cluster. Listens to changes in distributed data, exposes for assertions */
    private Harness harness;
    /** Will be started/stopped during test. Metadata from this should be cleaned up in distributed data by repairer */
    private CoreTopologyService cleanRestarter;
    /** Will be started/stopped during test. Does not cleanly leave, needs downing from repairer. Should be cleaned up by repairer */
    private CoreTopologyService uncleanRestarter;
    /** Should clean up for other members when they leave the cluster.*/
    private CoreTopologyService repairer;

    private int metadataCount = 2;
    private NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();

    @BeforeAll
    void setUp() throws Throwable
    {
        harness = new Harness();
        var cleanDiscoveryServiceFactory = new AkkaDiscoveryServiceFactory();
        repairer = coreTopologyService( harness.port, cleanDiscoveryServiceFactory );
        cleanRestarter = coreTopologyService( harness.port, cleanDiscoveryServiceFactory );
        uncleanRestarter = coreTopologyService( harness.port, new AkkaUncleanShutdownDiscoveryServiceFactory() );

        repairer.init();
        cleanRestarter.init();
        uncleanRestarter.init();

        repairer.start();
    }

    @AfterAll
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

    private CoreTopologyService coreTopologyService( int harnessPort, DiscoveryServiceFactory discoveryServiceFactory )
    {
        var discoverySocket = new SocketAddress( "localhost", PortAuthority.allocatePort() );
        var config = Config.newBuilder()
                .setDefaults( SERVER_DEFAULTS )
                .set( discovery_listen_address, discoverySocket )
                .set( discovery_advertised_address, discoverySocket )
                .set( initial_discovery_members, singletonList( new SocketAddress( "localhost" , harnessPort ) ) )
                .set( store_internal_log_level, Level.DEBUG )
                .set( middleware_logging_level, Level.DEBUG )
                .build();
        var memberId = new MemberId( UUID.randomUUID() );
        var logProvider = NullLogProvider.getInstance();
        var membersResolver = new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );
        var monitors = new Monitors();
        var retryStrategy = new RetryStrategy( 100, 3 );
        var sslPolicyLoader = SslPolicyLoader.create( config, logProvider );
        DiscoveryMemberFactory discoveryMemberFactory = ( MemberId mbr ) -> new TestDiscoveryMember( mbr, asSet( namedDatabaseId ) );

        return discoveryServiceFactory.coreTopologyService( config, memberId, createInitialisedScheduler(), logProvider,
                logProvider, membersResolver, retryStrategy, sslPolicyLoader, discoveryMemberFactory, monitors, Clocks.systemClock() );
    }

    private static class Harness
    {
        final int port = PortAuthority.allocatePort();
        static final Key<LWWMap<UniqueAddress,CoreServerInfoForMemberId>> KEY = LWWMapKey.create( "member-data" );
        LWWMap<?,?> replicatedData = LWWMap.empty();
        private final ActorSystemComponents actorSystemComponents;
        private final ExecutorService executor;

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
            executor = Executors.newWorkStealingPool();
            var actorSystemFactory = new ActorSystemFactory( Optional.empty(), executor, config, logProvider );
            actorSystemComponents = new ActorSystemComponents( actorSystemFactory, ProviderSelection.cluster() );
            actorSystemComponents.cluster().joinSeedNodes(
                    singletonList( new Address( AKKA_SCHEME, actorSystemComponents.cluster().system().name(), "localhost", port ) ) );

            var listener = actorSystemComponents.actorSystem().actorOf( ReplicatorListener.props( this ) );

            actorSystemComponents.replicator().tell( new Replicator.Subscribe<>( KEY, listener ), ActorRef.noSender() );
        }

        public void shutdown()
        {
            actorSystemComponents.coordinatedShutdown().runAll();
            executor.shutdown();
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