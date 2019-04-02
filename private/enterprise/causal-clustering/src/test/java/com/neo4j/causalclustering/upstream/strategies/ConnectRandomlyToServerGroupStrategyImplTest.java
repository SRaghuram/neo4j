/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;

import static co.unruly.matchers.OptionalMatchers.contains;
import static co.unruly.matchers.OptionalMatchers.empty;
import static com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isIn;

public class ConnectRandomlyToServerGroupStrategyImplTest
{
    @Test
    public void shouldStayWithinGivenSingleServerGroup()
    {
        // given
        final List<String> myServerGroup = Collections.singletonList( "my_server_group" );

        MemberId[] myGroupMemberIds = UserDefinedConfigurationStrategyTest.memberIDs( 10 );
        TopologyService topologyService = getTopologyService( myServerGroup, myGroupMemberIds, Collections.singletonList( "your_server_group" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroup, topologyService, myGroupMemberIds[0] );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, contains( isIn( myGroupMemberIds ) ) );
    }

    @Test
    public void shouldSelectAnyFromMultipleServerGroups()
    {
        // given
        final List<String> myServerGroups = Arrays.asList( "a", "b", "c" );

        MemberId[] myGroupMemberIds = UserDefinedConfigurationStrategyTest.memberIDs( 10 );
        TopologyService topologyService = getTopologyService( myServerGroups, myGroupMemberIds, Arrays.asList( "x", "y", "z" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroups, topologyService, myGroupMemberIds[0] );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, contains( isIn( myGroupMemberIds ) ) );
    }

    @Test
    public void shouldReturnEmptyIfNoGroupsInConfig()
    {
        // given
        MemberId[] myGroupMemberIds = UserDefinedConfigurationStrategyTest.memberIDs( 10 );
        TopologyService topologyService =
                getTopologyService( Collections.singletonList( "my_server_group" ), myGroupMemberIds, Arrays.asList( "x", "y", "z" ) );
        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( Collections.emptyList(), topologyService, null );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, empty() );
    }

    @Test
    public void shouldReturnEmptyIfGroupOnlyContainsSelf()
    {
        // given
        final List<String> myServerGroup = Collections.singletonList( "group" );

        MemberId[] myGroupMemberIds = UserDefinedConfigurationStrategyTest.memberIDs( 1 );
        TopologyService topologyService = getTopologyService( myServerGroup, myGroupMemberIds, Arrays.asList( "x", "y", "z" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroup, topologyService, myGroupMemberIds[0] );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, empty() );
    }

    static TopologyService getTopologyService( List<String> myServerGroups, MemberId[] myGroupMemberIds, List<String> unwanted )
    {
        return UserDefinedConfigurationStrategyTest.fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( myServerGroups, myGroupMemberIds, unwanted, 10 ) );
    }

    static ReadReplicaTopology fakeReadReplicaTopology( List<String> wanted, MemberId[] memberIds, List<String> unwanted, int unwantedNumber )
    {
        Map<MemberId,ReadReplicaInfo> readReplicas = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : memberIds )
        {
            readReplicas.put( memberId, new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ), new AdvertisedSocketAddress( "localhost", 10000 + offset ),
                    new HashSet<>( wanted ), new DatabaseId( "default" ) ) );

            offset++;
        }

        for ( int i = 0; i < unwantedNumber; i++ )
        {
            readReplicas.put( new MemberId( UUID.randomUUID() ), new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ), new AdvertisedSocketAddress( "localhost", 10000 + offset ),
                    new HashSet<>( unwanted ), new DatabaseId( "default" ) ) );

            offset++;
        }

        return new ReadReplicaTopology( readReplicas );
    }
}
