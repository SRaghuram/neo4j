/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertNotEquals;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.logging.NullLogProvider.getInstance;

@RunWith( Parameterized.class )
public class GetServersProcedureV1RoutingTest
{
    @Parameters
    public static Collection<Object> data()
    {
        return Arrays.asList( 1, 2 );
    } //the write endpoints are always index 0

    @Parameter
    public int serverClass;

    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );
    private Config config = Config.defaults();

    @Test
    public void shouldReturnEndpointsInDifferentOrders() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );
        coreMembers.put( member( 1 ), addressesForCore( 1, false ) );
        coreMembers.put( member( 2 ), addressesForCore( 2, false ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        AnyValue endpoints = getEndpoints( proc );

        //then
        AnyValue endpointsInDifferentOrder = getEndpoints( proc );
        for ( int i = 0; i < 100; i++ )
        {
            if ( endpointsInDifferentOrder.equals( endpoints ) )
            {
                endpointsInDifferentOrder = getEndpoints( proc );
            }
            else
            {
                //Different order of servers, no need to retry.
                break;
            }
        }
        assertNotEquals( endpoints, endpointsInDifferentOrder );
    }

    private AnyValue getEndpoints( LegacyGetServersProcedure proc )
            throws ProcedureException
    {
        List<AnyValue[]> results = asList( proc.apply( null, new AnyValue[0], null ) );
        AnyValue[] rows = results.get( 0 );
        ListValue servers = (ListValue) rows[1];
        MapValue endpoints = (MapValue) servers.value( serverClass );
        return endpoints.get( "addresses" );
    }
}
