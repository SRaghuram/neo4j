/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.neo4j.test.extension.Inject;

import static com.neo4j.configuration.CausalClusteringSettings.leader_failure_detection_window;
import static com.neo4j.configuration.CausalClusteringSettings.election_failure_detection_window;
import static com.neo4j.configuration.CausalClusteringSettings.minimum_core_cluster_size_at_formation;
import static com.neo4j.configuration.CausalClusteringSettings.minimum_core_cluster_size_at_runtime;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ClusterExtension
@TestInstance( PER_METHOD )
class ConsensusGroupSettingsIT
{
    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void shouldNotAllowTheConsensusGroupToDropBelowMinimumConsensusGroupSize() throws Exception
    {
        // given
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 5 )
                .withNumberOfReadReplicas( 0 )
                .withInstanceCoreParam( minimum_core_cluster_size_at_formation, value -> "5" )
                .withInstanceCoreParam( minimum_core_cluster_size_at_runtime, value -> "3" )
                .withInstanceCoreParam( leader_failure_detection_window, value -> "1s-2s" )
                .withInstanceCoreParam( election_failure_detection_window, value -> "1s-2s" );

        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        var numberOfCoreSeversToRemove = 3;

        // when
        for ( var i = 0; i < numberOfCoreSeversToRemove; i++ )
        {
            var leader = cluster.awaitLeader();
            cluster.removeCoreMember( leader );
        }

        // then
        var core = cluster.coreMembers().iterator().next();
        var raft = core.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class );
        assertEquals( 3, raft.replicationMembers().size() );
    }
}
