/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.plugins;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;
import org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerPoliciesPlugin;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.ConfigBuilder;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ServerShufflingTest
{
    @Test
    public void internalProcessorShouldShuffleServers() throws Exception
    {
        // given
        LoadBalancingProcessor delegate = mock( LoadBalancingPlugin.class );

        List<Endpoint> routers = asList(
                Endpoint.route( new AdvertisedSocketAddress( "route", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "route", 2 ) ) );
        List<Endpoint> writers = asList(
                Endpoint.write( new AdvertisedSocketAddress( "write", 3 ) ),
                Endpoint.write( new AdvertisedSocketAddress( "write", 4 ) ),
                Endpoint.write( new AdvertisedSocketAddress( "write", 5 ) ) );
        List<Endpoint> readers = asList(
                Endpoint.read( new AdvertisedSocketAddress( "read", 6 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 7 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 8 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 9 ) ) );

        long ttl = 1000;
        LoadBalancingProcessor.Result result = new LoadBalancingResult(
                new ArrayList<>( routers ),
                new ArrayList<>( writers ),
                new ArrayList<>( readers ),
                ttl );

        when( delegate.run( any() ) ).thenReturn( result );

        ServerShufflingProcessor plugin = new ServerShufflingProcessor( delegate );

        boolean completeShuffle = false;
        for ( int i = 0; i < 1000; i++ ) // we try many times to make false negatives extremely unlikely
        {
            // when
            LoadBalancingProcessor.Result shuffledResult = plugin.run( emptyMap() );

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
    public void serverPoliciesPluginShouldShuffleServers() throws Exception
    {
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );
        coreMembers.put( member( 1 ), addressesForCore( 1, false ) );
        coreMembers.put( member( 2 ), addressesForCore( 2, false ) );
        coreMembers.put( member( 3 ), addressesForCore( 3, false ) );
        coreMembers.put( member( 4 ), addressesForCore( 4, false ) );

        final CoreTopology clusterTopology = new CoreTopology( new ClusterId( UUID.randomUUID() ), false, coreMembers );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        ServerPoliciesPlugin serverPoliciesPlugin = new ServerPoliciesPlugin();

        stringMap(
                CausalClusteringSettings.load_balancing_shuffle.name(), "true",
                CausalClusteringSettings.cluster_routing_ttl.name(), "1s" );

        serverPoliciesPlugin.init( coreTopologyService, leaderLocator,
                NullLogProvider.getInstance(), Config.defaults( CausalClusteringSettings.load_balancing_shuffle, "true" ) );

        List<Endpoint> routers = endpointsOfType( coreMembers.values().stream(), Endpoint::route );
        CoreServerInfo leader = coreMembers.get( member( 0 ) );
        Endpoint writer = Endpoint.write( leader.connectors().boltAddress() );
        Stream<CoreServerInfo> followerStream = coreMembers.values().stream().filter( c -> !c.equals( leader ) );
        List<Endpoint> readers = endpointsOfType( followerStream , Endpoint::read );

        boolean completeShuffle = false;
        for ( int i = 0; i < 1000; i++ ) // we try many times to make false negatives extremely unlikely
        {
            // when
            LoadBalancingProcessor.Result shuffledResult = serverPoliciesPlugin.run( emptyMap() );

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

    private List<Endpoint> endpointsOfType( Stream<CoreServerInfo> coreMembers, Function<AdvertisedSocketAddress,Endpoint> transform )
    {
        return coreMembers.map( c -> transform.apply( c.connectors().boltAddress() ) ).collect( Collectors.toList() );
    }

}
