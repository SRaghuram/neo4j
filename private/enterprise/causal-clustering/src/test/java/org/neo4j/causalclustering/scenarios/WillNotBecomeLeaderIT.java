/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.graphdb.Node;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;

public class WillNotBecomeLeaderIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withDiscoveryServiceType( EnterpriseDiscoveryServiceType.HAZELCAST )
                    .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, "true" );

    @Rule
    public ExpectedException exceptionMatcher = ExpectedException.none();

    @Test
    public void clusterShouldNotElectNewLeader() throws Exception
    {
        // given
        int leaderId = 0;

        clusterRule.withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, x ->
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

        Cluster<?> cluster = clusterRule.createCluster();
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
        try
        {
            cluster.awaitLeader(10, SECONDS);

            fail( "Should not have elected a leader" );
        }
        catch ( TimeoutException ex )
        {
            // Successful
        }
    }
}
