/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverIds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectRandomlyWithinServerGroupStrategyTest
{
    private static final NamedDatabaseId DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();

    @Test
    void shouldUseServerGroupsFromConfig()
    {
        // given
        var myServerGroup = ServerGroupName.listOf( "my_server_group" );
        Config configWithMyServerGroup = Config.defaults( CausalClusteringSettings.server_groups, myServerGroup );
        Set<ServerId> myGroupServerIds = serverIds( 0, 10 );
        var topologyService = ConnectRandomlyToServerGroupStrategyImplTest.getTopologyService( Set.copyOf( myServerGroup ), myGroupServerIds,
                ServerGroupName.setOf( "your_server_group" ), Set.of( DATABASE_ID ) );

        ConnectRandomlyWithinServerGroupStrategy strategy = new ConnectRandomlyWithinServerGroupStrategy();
        strategy.inject( topologyService, configWithMyServerGroup, NullLogProvider.getInstance(), serverId( 0 ) );

        // when
        Optional<ServerId> result = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> results = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( results, everyItem( is( in( myGroupServerIds ) ) ) );
        assertThat( result, contains( is( in( myGroupServerIds ) ) ) );
    }

    @Test
    void shouldAdaptToChangesInConfig()
    {
        // given
        var myServerGroup = ServerGroupName.listOf( "my_server_group" );
        Config configWithMyServerGroup = Config.defaults( CausalClusteringSettings.server_groups, myServerGroup );
        Set<ServerId> myGroupServerIds = serverIds( 0, 10 );
        var topologyService = ConnectRandomlyToServerGroupStrategyImplTest.getTopologyService( Set.copyOf( myServerGroup ), myGroupServerIds,
                ServerGroupName.setOf( "your_server_group" ), Set.of( DATABASE_ID ) );

        ConnectRandomlyWithinServerGroupStrategy strategy = new ConnectRandomlyWithinServerGroupStrategy();
        strategy.inject( topologyService, configWithMyServerGroup, NullLogProvider.getInstance(), serverId( 0 ) );

        // when
        Optional<ServerId> result = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> results = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( results, everyItem( is( in( myGroupServerIds ) ) ) );
        assertThat( result, contains( is( in( myGroupServerIds ) ) ) );

        // when
        configWithMyServerGroup.set( CausalClusteringSettings.server_groups, List.of() );
        result = strategy.upstreamServerForDatabase( DATABASE_ID );
        results = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( results, empty() );
        assertTrue( result.isEmpty() );
    }

    @Test
    void filtersSelf()
    {
        // given
        var groupName = new ServerGroupName( "groupName" );
        Config config = Config.defaults( CausalClusteringSettings.server_groups, List.of( groupName ) );

        // and
        ConnectRandomlyWithinServerGroupStrategy connectRandomlyWithinServerGroupStrategy = new ConnectRandomlyWithinServerGroupStrategy();
        ServerId myself = new ServerId( new UUID( 123, 456 ) );
        connectRandomlyWithinServerGroupStrategy.inject( new TopologyServiceThatPrioritisesItself( myself, groupName ), config, NullLogProvider.getInstance(),
                myself );

        // when
        Optional<ServerId> result = connectRandomlyWithinServerGroupStrategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> results = connectRandomlyWithinServerGroupStrategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertFalse( results.isEmpty() );
        assertThat( myself, not( in( results ) ) );
        assertTrue( result.isPresent() );
        assertNotEquals( myself, result.get() );
    }
}
