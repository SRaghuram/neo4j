/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

import org.neo4j.graphdb.Node;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
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
            .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE );

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
            return "true";
        } );

        Cluster cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        assertEquals( leaderId, cluster.awaitLeader().serverId() );

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // When
        cluster.removeCoreMemberWithServerId( leaderId );

        // Then
        assertThrows( TimeoutException.class, () -> cluster.awaitLeader( 10, SECONDS ) );
    }
}
