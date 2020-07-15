/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static co.unruly.matchers.OptionalMatchers.contains;
import static co.unruly.matchers.OptionalMatchers.empty;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

class ConnectRandomlyToServerGroupStrategyImplTest
{
    private static final NamedDatabaseId DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();

    @Test
    void shouldStayWithinGivenSingleServerGroup()
    {
        // given
        final var myServerGroup = ServerGroupName.setOf( "my_server_group" );

        Set<MemberId> myGroupMemberIds = FakeTopologyService.memberIds( 0, 10 );
        var topologyService =
                getTopologyService( myServerGroup, myGroupMemberIds, ServerGroupName.setOf( "your_server_group" ), Set.of( DATABASE_ID ) );

        var strategy = new ConnectRandomlyToServerGroupImpl( () -> myServerGroup, topologyService, memberId( 0 ) );

        // when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, everyItem( is( in( myGroupMemberIds ) ) ) );
        assertThat( memberId, contains( is( in( myGroupMemberIds ) ) ) );
    }

    @Test
    void shouldSelectAnyFromMultipleServerGroups()
    {
        // given
        final var myServerGroups = ServerGroupName.setOf( "a", "b", "c" );

        Set<MemberId> myGroupMemberIds = FakeTopologyService.memberIds( 0, 10 );
        var topologyService = getTopologyService( myServerGroups, myGroupMemberIds, ServerGroupName.setOf( "x", "y", "z" ), Set.of( DATABASE_ID ) );

        var strategy = new ConnectRandomlyToServerGroupImpl( () -> myServerGroups, topologyService, memberId( 0 ) );

        // when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, everyItem( is( in( myGroupMemberIds ) ) ) );
        assertThat( memberId, contains( is( in( myGroupMemberIds ) ) ) );
    }

    @Test
    void shouldReturnEmptyIfNoGroupsInConfig()
    {
        // given
        Set<MemberId> myGroupMemberIds = FakeTopologyService.memberIds( 0, 10 );
        var topologyService =
                getTopologyService( ServerGroupName.setOf( "my_server_group" ), myGroupMemberIds, ServerGroupName.setOf( "x", "y", "z" ),
                        Set.of( DATABASE_ID ) );
        var strategy = new ConnectRandomlyToServerGroupImpl( Set::of, topologyService, null );

        // when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, emptyCollectionOf( MemberId.class ) );
        assertThat( memberId, empty() );
    }

    @Test
    void shouldReturnEmptyIfGroupOnlyContainsSelf()
    {
        // given
        final var myServerGroup = ServerGroupName.setOf( "group" );

        var myGroupMemberIds = singleton( memberId( 0 ) );
        var topologyService = getTopologyService( myServerGroup, myGroupMemberIds, ServerGroupName.setOf( "x", "y", "z" ), Set.of( DATABASE_ID ) );

        var strategy = new ConnectRandomlyToServerGroupImpl( () -> myServerGroup, topologyService, memberId( 0 ) );

        // when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, emptyCollectionOf( MemberId.class ) );
        assertThat( memberId, empty() );
    }

    static TopologyService getTopologyService( Set<ServerGroupName> myServerGroups, Set<MemberId> myGroupMemberIds, Set<ServerGroupName> unwanted,
            Set<NamedDatabaseId> namedDatabaseIds )
    {
        var thisCore = memberId( -1 );

        var otherReplicas = Stream.generate( UUID::randomUUID ).map( MemberId::new ).limit( 10 ).collect( Collectors.toSet() );

        var allReplicas = new HashSet<>( myGroupMemberIds );
        allReplicas.addAll( otherReplicas );

        var topologyService = new FakeTopologyService( singleton( thisCore ), allReplicas, thisCore, namedDatabaseIds );

        topologyService.setGroups( myGroupMemberIds, Set.copyOf( myServerGroups ) );
        topologyService.setGroups( otherReplicas, Set.copyOf( unwanted ) );
        return topologyService;
    }
}
