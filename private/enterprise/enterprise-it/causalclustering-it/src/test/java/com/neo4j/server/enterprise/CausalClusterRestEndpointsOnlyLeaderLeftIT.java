/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.test.extension.Inject;

import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.canVote;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.statusEndpoint;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.FALSE;
import static org.neo4j.test.conditions.Conditions.TRUE;

@ClusterExtension
class CausalClusterRestEndpointsOnlyLeaderLeftIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void setupClass() throws ExecutionException, InterruptedException
    {
        var clusterConfig = ClusterConfig
                .clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParam( CausalClusteringSettings.leader_failure_detection_window, "10s-12s" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        for ( var core : cluster.coreMembers() )
        {
            assertEventually( canVote( statusEndpoint( core, DEFAULT_DATABASE_NAME ) ), TRUE, 1, MINUTES );
        }
    }

    @AfterAll
    void shutdownClass()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void participatingInRaftGroupFalseWhenNotInGroup() throws TimeoutException
    {
        var cores = cluster.coreMembers();
        assertThat( cores, hasSize( greaterThan( 1 ) ) );
        var leader = cluster.awaitLeader( DEFAULT_DATABASE_NAME );
        // stop all cores except the leader
        cores.stream().filter( core -> !core.equals( leader ) ).forEach( ClusterMember::shutdown );
        assertEventually( canVote( statusEndpoint( leader, DEFAULT_DATABASE_NAME ) ), FALSE, 2, MINUTES );
    }
}
