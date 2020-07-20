/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftTestMember;
import com.neo4j.causalclustering.routing.load_balancing.DefaultLeaderService;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static com.neo4j.causalclustering.identity.RaftTestMember.leader;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.configuration.CausalClusteringSettings.load_balancing_shuffle;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class AddressCollectorTest
{
    private NamedDatabaseId namedDatabaseId = DatabaseIdFactory.from( "testDb", UUID.randomUUID() );
    private Log log = nullLogProvider().getLog( "ignore" );

    @Test
    void shouldProvideReaderAndRouterForSingleCoreSetup() throws Exception
    {
        // given
        var addressCollector = setup( true, true, 1, 0, -1 );
        var address0 = addressesForCore( 0, false ).boltAddress();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.routeEndpoints(), contains( address0 ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 0 ) );
        assertThat( routingResult.readEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.readEndpoints(), contains( address0 ) );
    }

    @Test
    void shouldReturnSelfIfOnlyMemberOfTheCluster() throws Exception
    {
        // given
        var addressCollector = setup( true, true, 1, 0, 0 );
        var address0 = addressesForCore( 0, false ).boltAddress();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.routeEndpoints(), contains( address0 ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( address0 ) );
        assertThat( routingResult.readEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.readEndpoints(), contains( address0 ) );
    }

    @Test
    void shouldReturnCoreServersWithRouteAllCoresButLeaderAsReadAndSingleWriteActions() throws Exception
    {
        // given
        var addressCollector = setup( true, true, 3, 0, 0 );
        var address0 = addressesForCore( 0, false ).boltAddress();
        var address1 = addressesForCore( 1, false ).boltAddress();
        var address2 = addressesForCore( 2, false ).boltAddress();
        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( address0, address1, address2 ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( address0 ) );
        assertThat( routingResult.readEndpoints(), hasSize( 2 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( address1, address2 ) );
    }

    @Test
    void shouldReturnReadReplicasAndFollowersAsReaders() throws Exception
    {
        // given
        var addressCollector = setup( true, true, 3, 3, 0 );
        var address0 = addressesForCore( 0, false ).boltAddress();
        var address1 = addressesForCore( 1, false ).boltAddress();
        var address2 = addressesForCore( 2, false ).boltAddress();
        var address3 = addressesForReadReplica( 3 ).boltAddress();
        var address4 = addressesForReadReplica( 4 ).boltAddress();
        var address5 = addressesForReadReplica( 5 ).boltAddress();
        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( address0, address1, address2 ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( address0 ) );
        assertThat( routingResult.readEndpoints(), hasSize( 5 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( address1, address2, address3, address4, address5 ) );
    }

    @Test
    void shouldReturnOnlyReadReplicasAsReaders() throws Exception
    {
        // given
        var addressCollector = setup( false, true, 3, 3, 0 );
        var address0 = addressesForCore( 0, false ).boltAddress();
        var address1 = addressesForCore( 1, false ).boltAddress();
        var address2 = addressesForCore( 2, false ).boltAddress();
        var address3 = addressesForReadReplica( 3 ).boltAddress();
        var address4 = addressesForReadReplica( 4 ).boltAddress();
        var address5 = addressesForReadReplica( 5 ).boltAddress();
        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( address0, address1, address2 ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( address0 ) );
        assertThat( routingResult.readEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( address3, address4, address5 ) );
    }

    @Test
    void shouldApplyPolicy() throws Exception
    {
        // given
        var addressCollector = setup( true, true, 3, 3, 0 );
        var address0 = addressesForCore( 0, false ).boltAddress();
        var address1 = addressesForCore( 1, false ).boltAddress();
        var address2 = addressesForCore( 2, false ).boltAddress();
        var address3 = addressesForReadReplica( 3 ).boltAddress();
        var address5 = addressesForReadReplica( 5 ).boltAddress();
        //policy filters out members with odd indexes
        var policy = new Policy()
        {
            @Override
            public Set<ServerInfo> apply( Set<ServerInfo> data )
            {
                return data.stream().filter( info -> (info.boltAddress().getPort() % 2) != 0 ).collect( Collectors.toSet() );
            }
        };

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, policy );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        // since policy selects odd numbered member as preferred, it must be ordered: first address1, afterwards order is not relevant
        assertThat( routingResult.routeEndpoints(), anyOf( contains( address1, address0, address2 ), contains( address1, address2, address0 ) ) );
        assertThat( routingResult.readEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( address1, address3, address5 ) );
    }

    @Test
    void shouldNotShuffleWithPolicy() throws Exception
    {
        // given
        var addressCollectorShuffled = setup( false, true, 1, 10, 0 );
        var addressCollectorNotShuffled = setup( false, false, 1, 10, 0 );
        // policy does nothing
        var policy = new Policy()
        {
            @Override
            public Set<ServerInfo> apply( Set<ServerInfo> data )
            {
                return data;
            }
        };

        // when
        var routingResultShuffledFirst = addressCollectorShuffled.createRoutingResult( namedDatabaseId, policy );
        var routingResultShuffledSecond = addressCollectorShuffled.createRoutingResult( namedDatabaseId, policy );

        var routingResultNotShuffledFirst = addressCollectorNotShuffled.createRoutingResult( namedDatabaseId, policy );
        var routingResultNotShuffledSecond = addressCollectorNotShuffled.createRoutingResult( namedDatabaseId, policy );

        // then
        assertThat( routingResultShuffledFirst.readEndpoints(), not( equalTo( routingResultShuffledSecond.readEndpoints() ) ) );
        assertThat( routingResultNotShuffledFirst.readEndpoints(), equalTo( routingResultNotShuffledSecond.readEndpoints() ) );
    }

    @Test
    void shouldReturnOnlyCoresThatAreOnline()
    {
        // given that one of the cores is offline
        var cores = createCores( 1 );
        var readReplicas = createReadReplicas( 1, 1 );

        var config = getConfig( true, true );

        var topologyService = new FakeTopologyService( cores.keySet(), readReplicas.keySet(), member( 0 ), Set.of( namedDatabaseId ) );
        var leaderService = new DefaultLeaderService( topologyService, nullLogProvider() );

        topologyService.setState( cores.keySet(), new DiscoveryDatabaseState( namedDatabaseId.databaseId(), STOPPED ) );
        topologyService.setState( readReplicas.keySet(), new DiscoveryDatabaseState( namedDatabaseId.databaseId(), STARTED ) );

        var addressCollector = new AddressCollector( topologyService, leaderService, config, log );
        var address0 = addressesForCore( 0, false ).boltAddress();
        var address1 = addressesForReadReplica( 1 ).boltAddress();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( address0 ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( address0 ) );
        assertThat( routingResult.readEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.readEndpoints(), contains( address1 ) );
    }

    @Test
    void shouldReturnOnlyReadReplicasThatAreOnline()
    {
        // given that all cores are online and one of the two read replicas is online
        var cores = createCores( 1 );
        var readReplicas = createReadReplicas( 1, 2 );
        var readMembers = new ArrayList<>( readReplicas.keySet() );

        var config = getConfig( false, true );

        var topologyService = new FakeTopologyService( cores.keySet(), readReplicas.keySet(), member( 0 ), Set.of( namedDatabaseId ) );
        var leaderService = new DefaultLeaderService( topologyService, nullLogProvider() );
        leaderService.onLeaderSwitch( namedDatabaseId, leader( 0, 1 ) );

        topologyService.setState( cores.keySet(), new DiscoveryDatabaseState( namedDatabaseId.databaseId(), STARTED ) );
        topologyService.setState( Set.of( readMembers.get( 0 ) ), new DiscoveryDatabaseState( namedDatabaseId.databaseId(), STARTED ) );
        topologyService.setState( Set.of( readMembers.get( 1 ) ), new DiscoveryDatabaseState( namedDatabaseId.databaseId(), STOPPED ) );

        var addressCollector = new AddressCollector( topologyService, leaderService, config, log );
        var address0 = addressesForCore( 0, false ).boltAddress();
        var address1 = addressesForReadReplica( 1 ).boltAddress();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( address0 ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( address0 ) );
        assertThat( routingResult.readEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.readEndpoints(), contains( address1 ) );
    }

    private AddressCollector setup( boolean allowReads, boolean shuffle, int numberOfCores, int numberOnReplicas, int leaderIndex )
    {
        var cores = createCores( numberOfCores );
        var readReplicas = createReadReplicas( numberOfCores, numberOnReplicas );

        return setup( allowReads, shuffle, leaderIndex, cores, readReplicas );
    }

    private AddressCollector setup( boolean allowReads,
                                    boolean shuffle,
                                    int leaderIndex,
                                    Map<MemberId,CoreServerInfo> cores,
                                    Map<MemberId,ReadReplicaInfo> readReplicas )
    {
        Set<MemberId> sortedCores =
                cores.keySet().stream().sorted( Comparator.comparingInt( m -> cores.get( m ).getRaftServer().getPort() ) ).collect(
                        Collectors.toCollection( LinkedHashSet::new ) );
        Set<MemberId> sortedRead =
                readReplicas.keySet().stream().sorted( Comparator.comparingInt( m -> readReplicas.get( m ).catchupServer().getPort() ) ).collect(
                        Collectors.toCollection( LinkedHashSet::new ) );

        FakeTopologyService topologyService = new FakeTopologyService( sortedCores, sortedRead, member( 1 ), Set.of( namedDatabaseId ) );
        topologyService.setState( sortedCores, new DiscoveryDatabaseState( namedDatabaseId.databaseId(), EnterpriseOperatorState.STARTED ) );
        topologyService.setState( sortedRead, new DiscoveryDatabaseState( namedDatabaseId.databaseId(), EnterpriseOperatorState.STARTED ) );

        DefaultLeaderService leaderService = new DefaultLeaderService( topologyService, nullLogProvider() );

        if ( leaderIndex >= 0 )
        {
            leaderService.onLeaderSwitch( namedDatabaseId, leader( leaderIndex, 1 ) );
        }
        else
        {
            topologyService.removeLeader();
        }

        Config config = getConfig( allowReads, shuffle );

        return new AddressCollector( topologyService, leaderService, config, log );
    }

    private Config getConfig( boolean allowReads, boolean shuffle )
    {
        var config = Config.defaults();
        config.set( cluster_allow_reads_on_followers, allowReads );
        config.set( load_balancing_shuffle, shuffle );
        return config;
    }

    private Map<MemberId,ReadReplicaInfo> createReadReplicas( int numberOfCores, int numberOnReplicas )
    {
        return IntStream.range( numberOfCores, numberOfCores + numberOnReplicas ).boxed()
                        .collect( Collectors.toMap( RaftTestMember::member, i -> addressesForReadReplica( i ) ) );
    }

    private Map<MemberId,CoreServerInfo> createCores( int numberOfCores )
    {
        return IntStream.range( 0, numberOfCores ).boxed()
                        .collect( Collectors.toMap( RaftTestMember::member, i -> addressesForCore( i, false ) ) );
    }
}
