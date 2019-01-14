/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.helpers.DataCreator;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.causalclustering.helpers.DataCreator.countNodes;

@RunWith( Parameterized.class )
public abstract class BaseClusterIpFamilyIT
{
    protected BaseClusterIpFamilyIT( DiscoveryServiceType discoveryServiceType, IpFamily ipFamily, boolean useWildcard )
    {
        clusterRule.withDiscoveryServiceType( discoveryServiceType );
        clusterRule.withIpFamily( ipFamily ).useWildcard( useWildcard );
    }

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 )
            .withSharedCoreParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedReadReplicaParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedCoreParam( CausalClusteringSettings.middleware_logging_level, "0" )
            .withSharedReadReplicaParam( CausalClusteringSettings.middleware_logging_level, "0" );;

    private Cluster<?> cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldSetupClusterWithIPv6() throws Exception
    {
        // given
        int numberOfNodes = 10;

        // when
        CoreClusterMember leader = DataCreator.createEmptyNodes( cluster, numberOfNodes );

        // then
        assertEquals( numberOfNodes, countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );
    }
}
