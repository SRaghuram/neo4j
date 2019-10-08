/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static co.unruly.matchers.OptionalMatchers.contains;
import static co.unruly.matchers.OptionalMatchers.empty;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

class ConnectRandomlyToServerGroupStrategyImplTest
{
    private static final DatabaseId DATABASE_ID = TestDatabaseIdRepository.randomDatabaseId();

    @Test
    void shouldStayWithinGivenSingleServerGroup()
    {
        // given
        final List<String> myServerGroup = List.of( "my_server_group" );

        Set<MemberId> myGroupMemberIds = FakeTopologyService.memberIds( 0, 10 );
        TopologyService topologyService = getTopologyService( myServerGroup, myGroupMemberIds, List.of( "your_server_group" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroup, topologyService, memberId( 0 ) );

        // when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, contains( is( in( myGroupMemberIds ) ) ) );
    }

    @Test
    void shouldSelectAnyFromMultipleServerGroups()
    {
        // given
        final List<String> myServerGroups = List.of( "a", "b", "c" );

        Set<MemberId> myGroupMemberIds = FakeTopologyService.memberIds( 0, 10 );
        TopologyService topologyService = getTopologyService( myServerGroups, myGroupMemberIds, List.of( "x", "y", "z" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroups, topologyService, memberId( 0 ) );

        // when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, contains( is( in( myGroupMemberIds ) ) ) );
    }

    @Test
    void shouldReturnEmptyIfNoGroupsInConfig()
    {
        // given
        Set<MemberId> myGroupMemberIds = FakeTopologyService.memberIds( 0, 10 );
        TopologyService topologyService =
                getTopologyService( singletonList( "my_server_group" ), myGroupMemberIds, List.of( "x", "y", "z" ) );
        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( Collections.emptyList(), topologyService, null );

        // when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, empty() );
    }

    @Test
    void shouldReturnEmptyIfGroupOnlyContainsSelf()
    {
        // given
        final List<String> myServerGroup = List.of( "group" );

        var myGroupMemberIds = singleton( memberId( 0 ) );
        TopologyService topologyService = getTopologyService( myServerGroup, myGroupMemberIds, List.of( "x", "y", "z" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroup, topologyService, memberId( 0 ) );

        // when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, empty() );
    }

    static TopologyService getTopologyService( List<String> myServerGroups, Set<MemberId> myGroupMemberIds, List<String> unwanted )
    {
        var thisCore =  memberId( -1 );

        var otherReplicas = Stream.generate( UUID::randomUUID )
                .map( MemberId::new )
                .limit( 10 )
                .collect( Collectors.toSet() );

        var allReplicas = new HashSet<>( myGroupMemberIds );
        allReplicas.addAll( otherReplicas );

        var topologyService =  new FakeTopologyService( singleton( thisCore ), allReplicas, thisCore,
                singleton( TestDatabaseIdRepository.randomDatabaseId() ) );

        topologyService.setGroups( myGroupMemberIds, Set.copyOf( myServerGroups ) );
        topologyService.setGroups( otherReplicas, Set.copyOf( unwanted ) );
        return topologyService;
    }
}
