/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.ServerGroupName;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberIds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectRandomlyToServerGroupStrategyTest
{
    private static final NamedDatabaseId DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();

    @Test
    void shouldConnectToGroupDefinedInStrategySpecificConfig()
    {
        // given
        var targetServerGroup = ServerGroupName.listOf( "target_server_group" );
        Config configWithTargetServerGroup = Config.defaults( CausalClusteringSettings.connect_randomly_to_server_group_strategy, targetServerGroup );
        Set<MemberId> targetGroupMemberIds = memberIds( 0, 10 );
        var topologyService =
                ConnectRandomlyToServerGroupStrategyImplTest.getTopologyService( Set.copyOf( targetServerGroup ), targetGroupMemberIds,
                        ServerGroupName.setOf( "your_server_group" ), Set.of( DATABASE_ID ) );

        ConnectRandomlyToServerGroupStrategy strategy = new ConnectRandomlyToServerGroupStrategy();
        strategy.inject( topologyService, configWithTargetServerGroup, NullLogProvider.getInstance(), memberId( 0 ) );

        // when
        Optional<MemberId> result = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( result, contains( is( in( targetGroupMemberIds ) ) ) );
    }

    @Test
    void doesNotConnectToSelf()
    {
        // given
        ConnectRandomlyToServerGroupStrategy connectRandomlyToServerGroupStrategy = new ConnectRandomlyToServerGroupStrategy();
        MemberId myself = new MemberId( new UUID( 1234, 5678 ) );

        // and
        LogProvider logProvider = NullLogProvider.getInstance();
        var config = Config.defaults( CausalClusteringSettings.connect_randomly_to_server_group_strategy, ServerGroupName.listOf( "firstGroup" ) );
        var topologyService = new TopologyServiceThatPrioritisesItself( myself, new ServerGroupName( "firstGroup" ) );
        connectRandomlyToServerGroupStrategy.inject( topologyService, config, logProvider, myself );

        // when
        Optional<MemberId> found = connectRandomlyToServerGroupStrategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertTrue( found.isPresent() );
        assertNotEquals( myself, found.get() );
    }
}
