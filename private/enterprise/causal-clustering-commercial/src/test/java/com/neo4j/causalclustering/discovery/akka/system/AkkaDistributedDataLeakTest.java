/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.causalclustering.discovery.akka.CommercialAkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import org.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import org.neo4j.causalclustering.discovery.TopologyServiceMultiRetryStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.assertion.Assert;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.discovery.akka.system.ClusterJoiningActor.AKKA_SCHEME;
import static java.util.Collections.singletonList;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
class AkkaDistributedDataLeakTest
{
    // Part of Akka cluster. Listens to changes in distributed data, exposes for assertions
    private Harness harness;
    // We be started/stopped during test. Metadata from this should be cleaned up in distributed data.
    private CoreTopologyService restarter;
    // If restarter doesn't clean up after itself then this should clean up for it when it leaves the cluster.
    private CoreTopologyService repairer;

    @BeforeAll
    void setUp() throws Throwable
    {
        harness = new Harness();
        repairer = lifecycle( harness.port );
        restarter = lifecycle( harness.port );

        repairer.init();
        restarter.init();

        repairer.start();
    }

    @AfterAll
    void tearDown() throws Throwable
    {
        harness.shutdown();
        repairer.stop();
        repairer.shutdown();
        restarter.shutdown();
    }

    // Needs hundreds of reps to have a good chance of replicating a leak
    @RepeatedTest( 10 )
    void shouldNotLeakMetadata() throws Throwable
    {
        restarter.start();

        Assert.assertEventually( () -> harness.replicatedData.size(), Matchers.equalTo( 2 ), 20, TimeUnit.SECONDS );

        restarter.stop();

        Assert.assertEventually( () -> harness.replicatedData.size(), Matchers.equalTo( 1 ), 20, TimeUnit.SECONDS );
    }

    private CoreTopologyService lifecycle( int port )
    {
        Config config = Config.builder()
                .withServerDefaults()
                .withSetting( CausalClusteringSettings.discovery_listen_address, "localhost:0" )
                .withSetting( CausalClusteringSettings.initial_discovery_members, "localhost:" + port )
                .withSetting( CausalClusteringSettings.middleware_logging_level, "0" )
                .withSetting( CausalClusteringSettings.disable_middleware_logging, "false" )
                .build();
        MemberId memberId = new MemberId( UUID.randomUUID() );
        LogProvider logProvider = NullLogProvider.getInstance();
        // LogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        InitialDiscoveryMembersResolver membersResolver = new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );
        Monitors monitors = new Monitors();
        TopologyServiceMultiRetryStrategy retryStrategy = new TopologyServiceMultiRetryStrategy( 100, 3, logProvider );

        return new CommercialAkkaDiscoveryServiceFactory().coreTopologyService( config, memberId, JobSchedulerFactory.createInitialisedScheduler(), logProvider,
                logProvider, membersResolver, retryStrategy, monitors, Clocks.systemClock() );
    }

    private static class Harness
    {
        final int port = PortAuthority.allocatePort();
        static final Key<LWWMap<UniqueAddress,CoreServerInfoForMemberId>> KEY = LWWMapKey.create( "member-data" );
        LWWMap<?,?> replicatedData = LWWMap.empty();
        private final ActorSystemComponents actorSystemComponents;

        Harness()
        {
            Config config = Config.builder()
                    .withSetting( CausalClusteringSettings.discovery_listen_address, "localhost:" + port )
                    .withSetting( CausalClusteringSettings.initial_discovery_members, "localhost:" + port )
                    .build();

            NullLogProvider logProvider = NullLogProvider.getInstance();
            ActorSystemFactory actorSystemFactory = new ActorSystemFactory( Optional.empty(), Executors.newWorkStealingPool(), config, logProvider );
            actorSystemComponents = new ActorSystemComponents( actorSystemFactory, ProviderSelection.cluster() );
            actorSystemComponents.cluster().joinSeedNodes(
                    singletonList( new Address( AKKA_SCHEME, actorSystemComponents.cluster().system().name(), "localhost", port ) ) );

            ActorRef listener = actorSystemComponents.actorSystem().actorOf( ReplicatorListener.props( this ) );

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
