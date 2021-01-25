/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static co.unruly.matchers.OptionalMatchers.contains;
import static co.unruly.matchers.OptionalMatchers.empty;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverId;
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

        Set<ServerId> myGroupServerIds = FakeTopologyService.serverIds( 0, 10 );
        var topologyService =
                getTopologyService( myServerGroup, myGroupServerIds, ServerGroupName.setOf( "your_server_group" ), Set.of( DATABASE_ID ) );

        var strategy = new ConnectRandomlyToServerGroupImpl( () -> myServerGroup, topologyService, serverId( 0 ) );

        // when
        Optional<ServerId> memberId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> memberIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, everyItem( is( in( myGroupServerIds ) ) ) );
        assertThat( memberId, contains( is( in( myGroupServerIds ) ) ) );
    }

    @Test
    void shouldSelectAnyFromMultipleServerGroups()
    {
        // given
        final var myServerGroups = ServerGroupName.setOf( "a", "b", "c" );

        Set<ServerId> myGroupServerIds = FakeTopologyService.serverIds( 0, 10 );
        var topologyService = getTopologyService( myServerGroups, myGroupServerIds, ServerGroupName.setOf( "x", "y", "z" ), Set.of( DATABASE_ID ) );

        var strategy = new ConnectRandomlyToServerGroupImpl( () -> myServerGroups, topologyService, serverId( 0 ) );

        // when
        Optional<ServerId> memberId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> memberIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, everyItem( is( in( myGroupServerIds ) ) ) );
        assertThat( memberId, contains( is( in( myGroupServerIds ) ) ) );
    }

    @Test
    void shouldReturnEmptyIfNoGroupsInConfig()
    {
        // given
        Set<ServerId> myGroupServerIds = FakeTopologyService.serverIds( 0, 10 );
        var topologyService =
                getTopologyService( ServerGroupName.setOf( "my_server_group" ), myGroupServerIds, ServerGroupName.setOf( "x", "y", "z" ),
                        Set.of( DATABASE_ID ) );
        var strategy = new ConnectRandomlyToServerGroupImpl( Set::of, topologyService, null );

        // when
        Optional<ServerId> memberId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> memberIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, emptyCollectionOf( ServerId.class ) );
        assertThat( memberId, empty() );
    }

    @Test
    void shouldReturnEmptyIfGroupOnlyContainsSelf()
    {
        // given
        final var myServerGroup = ServerGroupName.setOf( "group" );

        var myGroupServerIds = singleton( serverId( 0 ) );
        var topologyService = getTopologyService( myServerGroup, myGroupServerIds, ServerGroupName.setOf( "x", "y", "z" ), Set.of( DATABASE_ID ) );

        var strategy = new ConnectRandomlyToServerGroupImpl( () -> myServerGroup, topologyService, serverId( 0 ) );

        // when
        Optional<ServerId> memberId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> memberIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, emptyCollectionOf( ServerId.class ) );
        assertThat( memberId, empty() );
    }

    static TopologyService getTopologyService( Set<ServerGroupName> myServerGroups, Set<ServerId> myGroupServerIds, Set<ServerGroupName> unwanted,
            Set<NamedDatabaseId> namedDatabaseIds )
    {
        var thisCore = serverId( -1 );

        var otherReplicas = Stream.generate( IdFactory::randomServerId ).limit( 10 ).collect( Collectors.toSet() );

        var allReplicas = new HashSet<>( myGroupServerIds );
        allReplicas.addAll( otherReplicas );

        var topologyService = new FakeTopologyService( singleton( thisCore ), allReplicas, thisCore, namedDatabaseIds );

        topologyService.setGroups( myGroupServerIds, Set.copyOf( myServerGroups ) );
        topologyService.setGroups( otherReplicas, Set.copyOf( unwanted ) );
        return topologyService;
    }
}
