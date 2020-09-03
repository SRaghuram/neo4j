/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins;

import com.neo4j.causalclustering.discovery.ClientConnector;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerPoliciesPlugin;
import com.neo4j.configuration.CausalClusteringSettings;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.identity.RaftTestMember.server;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServerShufflingTest
{
    @Test
    void internalProcessorShouldShuffleServers() throws Exception
    {
        // given
        var delegate = mock( LoadBalancingPlugin.class );

        var routers = List.of(
                new SocketAddress( "route", 1 ),
                new SocketAddress( "route", 2 ) );
        var writers = List.of(
                new SocketAddress( "write", 3 ),
                new SocketAddress( "write", 4 ),
                new SocketAddress( "write", 5 ) );
        var readers = List.of(
                new SocketAddress( "read", 6 ),
                new SocketAddress( "read", 7 ),
                new SocketAddress( "read", 8 ),
                new SocketAddress( "read", 9 ) );

        var ttl = 1000;
        var result = new RoutingResult(
                new ArrayList<>( routers ),
                new ArrayList<>( writers ),
                new ArrayList<>( readers ),
                ttl );

        when( delegate.run( any(), any() ) ).thenReturn( result );

        var plugin = new ServerShufflingProcessor( delegate );

        var completeShuffle = false;
        for ( var i = 0; i < 1000; i++ ) // we try many times to make false negatives extremely unlikely
        {
            // when
            var shuffledResult = plugin.run( new TestDatabaseIdRepository().defaultDatabase(), MapValue.EMPTY );

            // then: should still contain the same endpoints
            assertThat( shuffledResult.routeEndpoints(), containsInAnyOrder( routers.toArray() ) );
            assertThat( shuffledResult.writeEndpoints(), containsInAnyOrder( writers.toArray() ) );
            assertThat( shuffledResult.readEndpoints(), containsInAnyOrder( readers.toArray() ) );
            assertEquals( shuffledResult.ttlMillis(), ttl );

            // but possibly in a different order
            var readersEqual = shuffledResult.readEndpoints().equals( readers );
            var writersEqual = shuffledResult.writeEndpoints().equals( writers );
            var routersEqual = shuffledResult.routeEndpoints().equals( routers );

            if ( !readersEqual && !writersEqual && !routersEqual )
            {
                // we don't stop until it is completely different
                completeShuffle = true;
                break;
            }
        }

        assertTrue( completeShuffle );
    }

    @Test
    void serverPoliciesPluginShouldShuffleServers() throws Exception
    {
        var namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();
        var coreTopologyService = mock( CoreTopologyService.class );

        var leaderId = server( 0 );
        var coreMembers = Map.of(
                leaderId, addressesForCore( 0, false ),
                server( 1 ), addressesForCore( 1, false ),
                server( 2 ), addressesForCore( 2, false ),
                server( 3 ), addressesForCore( 3, false ),
                server( 4 ), addressesForCore( 4, false )
        );

        var leaderService = mock( LeaderService.class );
        when( leaderService.getLeaderId( namedDatabaseId ) ).thenReturn( Optional.of( leaderId ) );
        when( leaderService.getLeaderBoltAddress( namedDatabaseId ) ).thenReturn( Optional.of( coreMembers.get( leaderId ).boltAddress() ) );

        var coreTopology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), RaftGroupId.from( namedDatabaseId.databaseId() ), coreMembers );
        when( coreTopologyService.coreTopologyForDatabase( namedDatabaseId ) ).thenReturn( coreTopology );
        when( coreTopologyService.readReplicaTopologyForDatabase( namedDatabaseId ) )
                .thenReturn( new DatabaseReadReplicaTopology( namedDatabaseId.databaseId(), emptyMap() ) );
        when( coreTopologyService.lookupDatabaseState( any(), any() ) )
                .thenReturn( new DiscoveryDatabaseState( namedDatabaseId.databaseId(), STARTED ) );

        var serverPoliciesPlugin = new ServerPoliciesPlugin();
        assertTrue( serverPoliciesPlugin.isShufflingPlugin() );

        serverPoliciesPlugin.init( coreTopologyService, leaderService, NullLogProvider.getInstance(),
                Config.defaults( CausalClusteringSettings.load_balancing_shuffle, true ) );

        var routers = coreMembers.values().stream().map( ClientConnector::boltAddress ).collect( toList() );
        var leader = coreMembers.get( leaderId );
        var writer = leader.connectors().clientBoltAddress();
        var followerStream = coreMembers.values().stream().filter( c -> !c.equals( leader ) );
        var readers = followerStream.map( ClientConnector::boltAddress ).collect( toList() );

        var completeShuffle = false;
        for ( var i = 0; i < 1000; i++ ) // we try many times to make false negatives extremely unlikely
        {
            // when
            var shuffledResult = serverPoliciesPlugin.run( namedDatabaseId, MapValue.EMPTY );

            // then: should still contain the same endpoints
            assertThat( shuffledResult.routeEndpoints(), containsInAnyOrder( routers.toArray() ) );
            assertThat( shuffledResult.writeEndpoints(), contains( writer ) );
            assertThat( shuffledResult.readEndpoints(), containsInAnyOrder( readers.toArray() ) );

            // but possibly in a different order
            var readersEqual = shuffledResult.readEndpoints().equals( readers );
            var routersEqual = shuffledResult.routeEndpoints().equals( routers );

            if ( !readersEqual && !routersEqual )
            {
                // we don't stop until it is completely different
                completeShuffle = true;
                break;
            }
        }

        assertTrue( completeShuffle );
    }
}
