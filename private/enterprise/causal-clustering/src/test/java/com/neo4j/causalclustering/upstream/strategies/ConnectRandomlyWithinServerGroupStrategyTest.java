/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberIds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectRandomlyWithinServerGroupStrategyTest
{
    private static final NamedDatabaseId DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();

    @Test
    void shouldUseServerGroupsFromConfig()
    {
        // given
        var myServerGroup = List.of( "my_server_group" );
        Config configWithMyServerGroup = Config.defaults( CausalClusteringSettings.server_groups, myServerGroup );
        Set<MemberId> myGroupMemberIds = memberIds( 0, 10 );
        TopologyService topologyService = ConnectRandomlyToServerGroupStrategyImplTest.getTopologyService( myServerGroup, myGroupMemberIds,
                        Collections.singletonList( "your_server_group" ), Set.of( DATABASE_ID ) );

        ConnectRandomlyWithinServerGroupStrategy strategy = new ConnectRandomlyWithinServerGroupStrategy();
        strategy.inject( topologyService, configWithMyServerGroup, NullLogProvider.getInstance(), memberId( 0 ) );

        // when
        Optional<MemberId> result = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( result, contains( is( in( myGroupMemberIds ) ) ) );
    }

    @Test
    void filtersSelf()
    {
        // given
        String groupName = "groupName";
        Config config = Config.defaults( CausalClusteringSettings.server_groups, List.of( groupName ) );

        // and
        ConnectRandomlyWithinServerGroupStrategy connectRandomlyWithinServerGroupStrategy = new ConnectRandomlyWithinServerGroupStrategy();
        MemberId myself = new MemberId( new UUID( 123, 456 ) );
        connectRandomlyWithinServerGroupStrategy.inject( new TopologyServiceThatPrioritisesItself( myself, groupName ), config, NullLogProvider.getInstance(),
                myself );

        // when
        Optional<MemberId> result = connectRandomlyWithinServerGroupStrategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertTrue( result.isPresent() );
        assertNotEquals( myself, result.get() );
    }
}
