/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.identity.RaftTestMember;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;

import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_leader;
import static com.neo4j.configuration.CausalClusteringSettings.load_balancing_shuffle;
import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED_DUMPED;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class AddressCollectorTest
{
    private final NamedDatabaseId namedDatabaseId = DatabaseIdFactory.from( "testDb", UUID.randomUUID() );
    private final Log log = nullLogProvider().getLog( "ignore" );

    @Test
    void shouldProvideReaderAndRouterForSingleCoreSetup() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .addFollowers( 1, STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( true, false, true, leaderService, clusterServerInfos );
        var coreAddresses = clusterServerInfos.cores().allServers().stream().map( ServerInfo::boltAddress ).toArray();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.routeEndpoints(), contains( coreAddresses ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 0 ) );
        assertThat( routingResult.readEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.readEndpoints(), contains( coreAddresses ) );
    }

    @Test
    void shouldReturnSelfIfOnlyMemberOfTheCluster() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .setLeader( STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( true, false, true, leaderService, clusterServerInfos );
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.routeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.readEndpoints(), contains( leaderAddress ) );
    }

    @Test
    void shouldReturnCoreServersWithRouteAllCoresButLeaderAsReadAndSingleWriteActions() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .addFollowers( 2, STARTED )
                .setLeader( STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( true, false, true, leaderService, clusterServerInfos );
        var followerAddresses = clusterServerInfos.followers().onlineServers().stream().map( ServerInfo::boltAddress ).toArray();
        var coreAddresses = clusterServerInfos.cores().allServers().stream().map( ServerInfo::boltAddress ).toArray();
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( coreAddresses ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 2 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( followerAddresses ) );
    }

    @Test
    void shouldReturnCoreServersWithRouteAndReadAllCores() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .addFollowers( 2, STARTED )
                .setLeader( STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( true, true, true, leaderService, clusterServerInfos );
        var coreAddresses = clusterServerInfos.cores().onlineServers().stream().map( ServerInfo::boltAddress ).toArray();
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( coreAddresses ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( coreAddresses ) );
    }

    @Test
    void shouldReturnReadReplicasAndFollowersAsReaders() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .addFollowers( 2, STARTED )
                .setLeader( STARTED )
                .addReadReplicas( 3, STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( true, false, true, leaderService, clusterServerInfos );
        var coreAddresses = clusterServerInfos.cores().onlineServers().stream().map( ServerInfo::boltAddress ).toArray();
        var followersAddressesStream = clusterServerInfos.followers().onlineServers().stream().map( ServerInfo::boltAddress );
        var readReplicaAddressesStream = clusterServerInfos.readReplicas().onlineServers().stream().map( ServerInfo::boltAddress );

        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( coreAddresses ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 5 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( Stream.concat( followersAddressesStream, readReplicaAddressesStream ).toArray() ) );
    }

    @Test
    void shouldReturnReadReplicasAndLeaderAsReaders() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .addFollowers( 2, STARTED )
                .setLeader( STARTED )
                .addReadReplicas( 3, STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( false, true, true, leaderService, clusterServerInfos );
        var coreAddresses = clusterServerInfos.cores().onlineServers().stream().map( ServerInfo::boltAddress ).toArray();
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();
        var readers = clusterServerInfos.readReplicas().onlineServers().stream().map( ServerInfo::boltAddress ).collect( toList() );
        readers.add( leaderAddress );

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( coreAddresses ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 4 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( readers.toArray() ) );
    }

    @Test
    void shouldReturnReadReplicasAndFollowersAndLeaderAsReaders() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .addFollowers( 2, STARTED )
                .setLeader( STARTED )
                .addReadReplicas( 3, STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( true, true, true, leaderService, clusterServerInfos );
        var coreAddresses = clusterServerInfos.cores().onlineServers().stream().map( ServerInfo::boltAddress ).toArray();
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();
        var readers = Stream.concat( clusterServerInfos.readReplicas().onlineServers().stream(), clusterServerInfos.cores().onlineServers().stream() )
                .map( ServerInfo::boltAddress )
                .toArray();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( coreAddresses ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 6 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( readers ) );
    }

    @Test
    void shouldReturnOnlyReadReplicasAsReaders() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .addFollowers( 2, STARTED )
                .setLeader( STARTED )
                .addReadReplicas( 3, STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( false, false, true, leaderService, clusterServerInfos );
        var coreAddresses = clusterServerInfos.cores().onlineServers().stream().map( ServerInfo::boltAddress ).toArray();
        var readReplicaAddresses = clusterServerInfos.readReplicas().onlineServers().stream().map( ServerInfo::boltAddress ).toArray();
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();
        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( coreAddresses ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( readReplicaAddresses ) );
    }

    @Test
    void shouldApplyPolicy() throws Exception
    {
        // given
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .addFollowers( 2, STARTED )
                .setLeader( STARTED )
                .addReadReplicas( 3, STARTED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );
        var routingOrder = clusterServerInfos.cores().onlineServers().stream().sorted( ( o1, o2 ) ->
        {
            // if the policy filter is true this address should take precedence. Otherwise same order should apply.
            var prio1 = filterUneven().test( o1 );
            var prio2 = filterUneven().test( o2 );
            if ( prio1 )
            {
                return -1;
            }
            if ( prio2 )
            {
                return 1;
            }
            return 0;
        } ).map( ServerInfo::boltAddress ).toArray();
        var coreAddresses = clusterServerInfos.cores().onlineServers().stream().filter( filterUneven() ).map( ServerInfo::boltAddress );
        var readReplicaAddresses = clusterServerInfos.readReplicas().onlineServers().stream().filter( filterUneven() ).map( ServerInfo::boltAddress );
        var addressCollector = setup( true, true, false, leaderService, clusterServerInfos );

        //policy filters out members with odd indexes
        var policy = new Policy()
        {
            @Override
            public Set<ServerInfo> apply( Set<ServerInfo> data )
            {
                return data.stream().filter( filterUneven() ).collect( Collectors.toSet() );
            }
        };

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, policy );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 3 ) );
        // since policy selects odd numbered member as preferred, it must be ordered: first address1, afterwards order is not relevant
        assertThat( routingResult.routeEndpoints(), contains( routingOrder ) );
        assertThat( routingResult.readEndpoints(), hasSize( 3 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( Stream.concat( coreAddresses, readReplicaAddresses ).toArray() ) );
    }

    private Predicate<ServerInfo> filterUneven()
    {
        return info -> (info.boltAddress().getPort() % 2) != 0;
    }

    @Test
    void shouldNotShuffleWithPolicy() throws Exception
    {
        // given
        var clusterServerInfos1 = new StubClusterServerInfoBuilder()
                .setLeader( STARTED )
                .addReadReplicas( 10, STARTED )
                .build();

        var leaderService1 = createLeaderService( clusterServerInfos1 );

        var addressCollectorShuffled = setup( false, false, true, leaderService1, clusterServerInfos1 );
        var clusterServerInfos2 = new StubClusterServerInfoBuilder()
                .setLeader( STARTED )
                .addReadReplicas( 10, STARTED )
                .build();

        var leaderService2 = createLeaderService( clusterServerInfos1 );

        var addressCollectorNotShuffled = setup( false, false, false, leaderService2, clusterServerInfos2 );
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
    void shouldOnlyReturnOnlineServersAsReadersButAllCoresAsRouters()
    {
        // given that one of the cores is offline
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .setLeader( STARTED )
                .addFollowers( 1, STARTED )
                .addFollowers( 2, STORE_COPYING )
                .addFollowers( 1, STOPPED )
                .addFollowers( 1, DROPPED_DUMPED )
                .addReadReplicas( 2, STARTED )
                .addReadReplicas( 2, INITIAL )
                .addReadReplicas( 2, DROPPED )
                .addReadReplicas( 2, DIRTY )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( true, true, true, leaderService, clusterServerInfos );
        var allCores = clusterServerInfos.cores().allServers().stream().map( ServerInfo::boltAddress ).toArray();
        var onlineCores = clusterServerInfos.cores().onlineServers().stream().map( ServerInfo::boltAddress );
        var onlineReadReplicas = clusterServerInfos.readReplicas().onlineServers().stream().map( ServerInfo::boltAddress );
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 6 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( allCores ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 4 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( Stream.concat( onlineCores, onlineReadReplicas ).toArray() ) );
    }

    @Test
    void shouldFallbackToCoresIfNoReadReplicasAreOnline()
    {
        // given that one of the cores is offline
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .setLeader( STARTED )
                .addFollowers( 1, STARTED )
                .addReadReplicas( 2, DROPPED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( false, false, true, leaderService, clusterServerInfos );
        var allCores = clusterServerInfos.cores().allServers().stream().map( ServerInfo::boltAddress ).toArray();
        var onlineFollowers = clusterServerInfos.followers().onlineServers().stream().map( ServerInfo::boltAddress ).toArray();
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 2 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( allCores ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.readEndpoints(), containsInAnyOrder( onlineFollowers ) );
    }

    @Test
    void shouldFallbackToLeaderIfNoReadReplicasOrFollowersAreOnline()
    {
        // given that one of the cores is offline
        var clusterServerInfos = new StubClusterServerInfoBuilder()
                .setLeader( STARTED )
                .addFollowers( 1, STOPPED )
                .addReadReplicas( 2, DROPPED )
                .build();

        var leaderService = createLeaderService( clusterServerInfos );

        var addressCollector = setup( false, false, true, leaderService, clusterServerInfos );
        var allCores = clusterServerInfos.cores().allServers().stream().map( ServerInfo::boltAddress ).toArray();
        var leaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId ).orElseThrow();

        // when
        var routingResult = addressCollector.createRoutingResult( namedDatabaseId, null );

        // then
        assertThat( routingResult.routeEndpoints(), hasSize( 2 ) );
        assertThat( routingResult.routeEndpoints(), containsInAnyOrder( allCores ) );
        assertThat( routingResult.writeEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.writeEndpoints(), contains( leaderAddress ) );
        assertThat( routingResult.readEndpoints(), hasSize( 1 ) );
        assertThat( routingResult.readEndpoints(), contains( leaderAddress ) );
    }

    private LeaderService createLeaderService( ClusterServerInfos clusterServerInfos )
    {
        var serverInfo = clusterServerInfos.leader().allServers().stream().findFirst();
        return new StubLeaderService( serverInfo.map( ServerInfo::serverId ).orElse( null ), serverInfo.map( ServerInfo::boltAddress ).orElse( null ) );
    }

    private AddressCollector setup( boolean allowReadsOnFollowers,
            boolean allowReadsOnLeader,
            boolean shuffle,
            LeaderService leaderService,
            ClusterServerInfos clusterServerInfos )
    {
        Config config = getConfig( allowReadsOnFollowers, allowReadsOnLeader, shuffle );

        return new AddressCollector( dbId -> clusterServerInfos, leaderService, config, log );
    }

    private Config getConfig( boolean allowReadsOnFollowers, boolean allowReadsOnLeader, boolean shuffle )
    {
        var config = Config.defaults();
        config.set( cluster_allow_reads_on_followers, allowReadsOnFollowers );
        config.set( cluster_allow_reads_on_leader, allowReadsOnLeader );
        config.set( load_balancing_shuffle, shuffle );
        return config;
    }

    private static class StubClusterServerInfoBuilder
    {
        Map<EnterpriseOperatorState,Integer> cores = new HashMap<>();
        Map<EnterpriseOperatorState,Integer> readReplicas = new HashMap<>();
        private EnterpriseOperatorState leader;

        StubClusterServerInfoBuilder addFollowers( int amount, EnterpriseOperatorState state )
        {
            addServers( amount, state, cores );
            return this;
        }

        StubClusterServerInfoBuilder addReadReplicas( int amount, EnterpriseOperatorState state )
        {
            addServers( amount, state, readReplicas );
            return this;
        }

        StubClusterServerInfoBuilder setLeader( EnterpriseOperatorState state )
        {
            this.leader = state;
            return this;
        }

        ClusterServerInfos build()
        {
            if ( leader != null )
            {
                addServers( 1, leader, cores );
            }
            var nextId = new MutableInt( 0 );
            var coreServerInfos = cores.entrySet().stream().flatMap( entry -> IntStream.range( 0, entry.getValue() ).map( ignore -> nextId.getAndIncrement() )
                    .mapToObj( id -> Pair.of( new ServerInfo( TestTopology.addressesForCore( id ).boltAddress(), RaftTestMember.server( id ), Set.of() ),
                            entry.getKey() ) ) ).collect( Collectors.toMap( Pair::first, Pair::other ) );

            var readReplicaServerInfos =
                    readReplicas.entrySet().stream().flatMap( entry -> IntStream.range( 0, entry.getValue() ).map( ignore -> nextId.getAndIncrement() )
                            .mapToObj( id -> Pair.of(
                                    new ServerInfo( TestTopology.addressesForReadReplica( id ).boltAddress(), RaftTestMember.server( id ), Set.of() ),
                                    entry.getKey() ) ) ).collect( Collectors.toMap( Pair::first, Pair::other ) );

            ServerId leaderId = null;
            if ( leader != null )
            {
                leaderId =
                        coreServerInfos.entrySet().stream().filter( e -> e.getValue() == leader ).findFirst().map( e -> e.getKey().serverId() ).orElseThrow();
            }
            return new ClusterServerInfos( new ClusterServerInfos.ServerInfos( coreServerInfos ),
                    new ClusterServerInfos.ServerInfos( readReplicaServerInfos ), leaderId );
        }

        private static void addServers( int amount, EnterpriseOperatorState state, Map<EnterpriseOperatorState,Integer> serverMap )
        {
            var current = serverMap.getOrDefault( state, 0 );
            serverMap.put( state, current + amount );
        }
    }

    private static class StubLeaderService implements LeaderService
    {
        private final ServerId leaderId;
        private final SocketAddress boltAddress;

        private StubLeaderService( ServerId leaderId, SocketAddress boltAddress )
        {
            this.leaderId = leaderId;
            this.boltAddress = boltAddress;
        }

        @Override
        public Optional<ServerId> getLeaderId( NamedDatabaseId namedDatabaseId )
        {
            return Optional.ofNullable( leaderId );
        }

        @Override
        public Optional<SocketAddress> getLeaderBoltAddress( NamedDatabaseId namedDatabaseId )
        {
            return Optional.ofNullable( boltAddress );
        }
    }
}
