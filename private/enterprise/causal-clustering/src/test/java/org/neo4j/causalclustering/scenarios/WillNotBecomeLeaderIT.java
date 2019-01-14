/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.test.causalclustering.ClusterConfig;
import org.neo4j.test.causalclustering.ClusterExtension;
import org.neo4j.test.causalclustering.ClusterFactory;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.Label.label;

@ClusterExtension
class WillNotBecomeLeaderIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withDiscoveryServiceType( EnterpriseDiscoveryServiceType.HAZELCAST )
            .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, "true" );

    @Test
    void clusterShouldNotElectNewLeader() throws Exception
    {
        // given
        int leaderId = 0;

        clusterConfig.withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, x ->
        {
            if ( x == leaderId )
            {
                return "false";
            }
            else
            {
                return "true";
            }
        } );

        Cluster<?> cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        assertEquals( leaderId, cluster.awaitLeader().serverId() );

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // When
        cluster.removeCoreMemberWithServerId( leaderId );

        // Then
        assertThrows( TimeoutException.class, () -> cluster.awaitLeader( 10, SECONDS ) );
    }
}
