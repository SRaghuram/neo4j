/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.neo4j.logging.Level;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;

@ClusterExtension
abstract class BaseClusterIpFamilyIT
{
    BaseClusterIpFamilyIT( DiscoveryServiceType discoveryServiceType, IpFamily ipFamily, boolean useWildcard )
    {
        clusterConfig.withDiscoveryServiceType( discoveryServiceType );
        clusterConfig.withIpFamily( ipFamily ).useWildcard( useWildcard );
    }

    @Inject
    private ClusterFactory clusterFactory;

    private final ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 2 )
            .withSharedCoreParam( CausalClusteringSettings.middleware_logging_level, Level.DEBUG.toString() )
            .withSharedReadReplicaParam( CausalClusteringSettings.middleware_logging_level, Level.DEBUG.toString() );

    private Cluster cluster;

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
        Assertions.assertEquals( numberOfNodes, DataCreator.countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );
    }
}
