/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.helpers.DataCreator;
import org.neo4j.test.causalclustering.ClusterConfig;
import org.neo4j.test.causalclustering.ClusterExtension;
import org.neo4j.test.causalclustering.ClusterFactory;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static org.neo4j.causalclustering.helpers.DataCreator.countNodes;

@ClusterExtension
public abstract class BaseClusterIpFamilyIT
{
    protected BaseClusterIpFamilyIT( DiscoveryServiceType discoveryServiceType, IpFamily ipFamily, boolean useWildcard )
    {
        clusterConfig.withDiscoveryServiceType( discoveryServiceType );
        clusterConfig.withIpFamily( ipFamily ).useWildcard( useWildcard );
    }

    @Inject
    private ClusterFactory clusterFactory;

    private final ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 )
            .withSharedCoreParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedReadReplicaParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedCoreParam( CausalClusteringSettings.middleware_logging_level, "0" )
            .withSharedReadReplicaParam( CausalClusteringSettings.middleware_logging_level, "0" );

    private Cluster<?> cluster;

    @BeforeAll
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldSetupClusterWithIPv6() throws Exception
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
