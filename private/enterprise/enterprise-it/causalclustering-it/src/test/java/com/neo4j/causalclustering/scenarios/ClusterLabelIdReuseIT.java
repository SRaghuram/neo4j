/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.removeCheckPointFromDefaultDatabaseTxLog;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.time.Duration.ofMinutes;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.graphdb.Label.label;

@ClusterExtension
class ClusterLabelIdReuseIT
{
    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    @BeforeAll
    static void beforeAll() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldReuseCorrectlyAfterRecovery() throws Exception
    {
        // make leader1 allocate a batch of IDs for its transaction
        var leader1 = createLabelWithTimeout( "Label-1" );
        leader1.shutdown();

        // make leader2 allocate a batch of IDs for its transaction
        var leader2 = createLabelWithTimeout( "Label-2" );
        leader1.start();

        // make leader2 perform recovery
        leader2.shutdown();
        removeCheckPointFromDefaultDatabaseTxLog( leader2 );
        leader2.start();

        // make leader1 perform recovery
        leader1.shutdown();
        removeCheckPointFromDefaultDatabaseTxLog( leader1 );

        // create a label via leader2
        createLabelWithTimeout( "Label-3" );

        // start leader1 so it does recovery
        leader1.start();
        // make leader1 become a leader
        leader2.shutdown();

        // create a label via leader1
        createLabelWithTimeout( "Label-4" );
    }

    private CoreClusterMember createLabelWithTimeout( String name )
    {
        // use timeout to make the test fail even if a transaction gets stuck, for example because of database panic
        return assertTimeoutPreemptively( ofMinutes( 3 ), () -> createLabel( name ) );
    }

    private CoreClusterMember createLabel( String name ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            tx.createNode( label( name ) );
            tx.commit();
        } );
    }
}
