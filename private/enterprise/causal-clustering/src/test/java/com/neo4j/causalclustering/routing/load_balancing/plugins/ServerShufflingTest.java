/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.ClientConnector;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerPoliciesPlugin;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class ServerShufflingTest
{
    @Test
    void internalProcessorShouldShuffleServers() throws Exception
    {
        // given
        LoadBalancingProcessor delegate = mock( LoadBalancingPlugin.class );

        List<AdvertisedSocketAddress> routers = asList(
                new AdvertisedSocketAddress( "route", 1 ),
                new AdvertisedSocketAddress( "route", 2 ) );
        List<AdvertisedSocketAddress> writers = asList(
                new AdvertisedSocketAddress( "write", 3 ),
                new AdvertisedSocketAddress( "write", 4 ),
                new AdvertisedSocketAddress( "write", 5 ) );
        List<AdvertisedSocketAddress> readers = asList(
                new AdvertisedSocketAddress( "read", 6 ),
                new AdvertisedSocketAddress( "read", 7 ),
                new AdvertisedSocketAddress( "read", 8 ),
                new AdvertisedSocketAddress( "read", 9 ) );

        long ttl = 1000;
        RoutingResult result = new RoutingResult(
                new ArrayList<>( routers ),
                new ArrayList<>( writers ),
                new ArrayList<>( readers ),
                ttl );

        when( delegate.run( anyString(), any() ) ).thenReturn( result );

        ServerShufflingProcessor plugin = new ServerShufflingProcessor( delegate );

        boolean completeShuffle = false;
        for ( int i = 0; i < 1000; i++ ) // we try many times to make false negatives extremely unlikely
        {
            // when
            RoutingResult shuffledResult = plugin.run( DEFAULT_DATABASE_NAME, MapValue.EMPTY );

            // then: should still contain the same endpoints
            assertThat( shuffledResult.routeEndpoints(), containsInAnyOrder( routers.toArray() ) );
            assertThat( shuffledResult.writeEndpoints(), containsInAnyOrder( writers.toArray() ) );
            assertThat( shuffledResult.readEndpoints(), containsInAnyOrder( readers.toArray() ) );
            assertEquals( shuffledResult.ttlMillis(), ttl );

            // but possibly in a different order
            boolean readersEqual = shuffledResult.readEndpoints().equals( readers );
            boolean writersEqual = shuffledResult.writeEndpoints().equals( writers );
            boolean routersEqual = shuffledResult.routeEndpoints().equals( routers );

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
        DatabaseId databaseId = new DatabaseId( DEFAULT_DATABASE_NAME );
        CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        var leaderId = member( 0 );
        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( leaderId, addressesForCore( 0, false ) );
        coreMembers.put( member( 1 ), addressesForCore( 1, false ) );
        coreMembers.put( member( 2 ), addressesForCore( 2, false ) );
        coreMembers.put( member( 3 ), addressesForCore( 3, false ) );
        coreMembers.put( member( 4 ), addressesForCore( 4, false ) );

        LeaderService leaderService = mock( LeaderService.class );
        when( leaderService.getLeaderId( databaseId ) ).thenReturn( Optional.of( leaderId ) );
        when( leaderService.getLeaderBoltAddress( databaseId ) ).thenReturn( Optional.of( coreMembers.get( leaderId ).boltAddress() ) );

        final CoreTopology clusterTopology = new CoreTopology( databaseId, new ClusterId( UUID.randomUUID() ), false, coreMembers );
        when( coreTopologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( clusterTopology );
        when( coreTopologyService.readReplicaTopologyForDatabase( databaseId ) ).thenReturn( new ReadReplicaTopology( databaseId, emptyMap() ) );

        ServerPoliciesPlugin serverPoliciesPlugin = new ServerPoliciesPlugin();
        assertTrue( serverPoliciesPlugin.isShufflingPlugin() );

        serverPoliciesPlugin.init( coreTopologyService, leaderService,
                NullLogProvider.getInstance(), Config.defaults( CausalClusteringSettings.load_balancing_shuffle, "true" ) );

        List<AdvertisedSocketAddress> routers = coreMembers.values().stream().map( ClientConnector::boltAddress ).collect( Collectors.toList() );
        CoreServerInfo leader = coreMembers.get( leaderId );
        AdvertisedSocketAddress writer = leader.connectors().boltAddress();
        Stream<CoreServerInfo> followerStream = coreMembers.values().stream().filter( c -> !c.equals( leader ) );
        List<AdvertisedSocketAddress> readers = followerStream.map( ClientConnector::boltAddress ).collect( Collectors.toList() );

        boolean completeShuffle = false;
        for ( int i = 0; i < 1000; i++ ) // we try many times to make false negatives extremely unlikely
        {
            // when
            RoutingResult shuffledResult = serverPoliciesPlugin.run( databaseId.name(), MapValue.EMPTY );

            // then: should still contain the same endpoints
            assertThat( shuffledResult.routeEndpoints(), containsInAnyOrder( routers.toArray() ) );
            assertThat( shuffledResult.writeEndpoints(), contains( writer ) );
            assertThat( shuffledResult.readEndpoints(), containsInAnyOrder( readers.toArray() ) );

            // but possibly in a different order
            boolean readersEqual = shuffledResult.readEndpoints().equals( readers );
            boolean routersEqual = shuffledResult.routeEndpoints().equals( routers );

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
