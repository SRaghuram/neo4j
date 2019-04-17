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
import java.util.Optional;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static com.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.memberIDs;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isIn;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectRandomlyWithinServerGroupStrategyTest
{
    private static final DatabaseId DATABASE_ID = new DatabaseId( "orders" );

    @Test
    void shouldUseServerGroupsFromConfig()
    {
        // given
        final String myServerGroup = "my_server_group";
        Config configWithMyServerGroup = Config.defaults( CausalClusteringSettings.server_groups, myServerGroup );
        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService =
                ConnectRandomlyToServerGroupStrategyImplTest.getTopologyService( Collections.singletonList( myServerGroup ), myGroupMemberIds,
                        Collections.singletonList( "your_server_group" ) );

        ConnectRandomlyWithinServerGroupStrategy strategy = new ConnectRandomlyWithinServerGroupStrategy();
        strategy.inject( topologyService, configWithMyServerGroup, NullLogProvider.getInstance(), myGroupMemberIds[0] );

        // when
        Optional<MemberId> result = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( result, contains( isIn( myGroupMemberIds ) ) );
    }

    @Test
    void filtersSelf()
    {
        // given
        String groupName = "groupName";
        Config config = Config.defaults();
        config.augment( CausalClusteringSettings.server_groups, groupName );

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
