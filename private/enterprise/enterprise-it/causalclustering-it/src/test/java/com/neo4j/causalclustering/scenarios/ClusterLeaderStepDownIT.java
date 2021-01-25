/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.conditions.Conditions.FALSE;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class ClusterLeaderStepDownIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void beforeAll() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 8 ).withNumberOfReadReplicas( 0 ) );
        cluster.start();
    }

    @Test
    void leaderShouldStepDownWhenFollowersAreGone() throws Throwable
    {
        //Do some work to make sure the cluster is operating normally.
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode( Label.label( "bam" ) );
            node.setProperty( "bam", "bam" );
            tx.commit();
        } );

        Callable<List<CoreClusterMember>> followers = () -> cluster.coreMembers().stream().filter(
                m -> m.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class ).currentRole() != Role.LEADER ).collect( toList() );
        assertEventually( "All followers visible", followers, new HamcrestCondition<>( Matchers.hasSize( 7 ) ), 2, TimeUnit.MINUTES );

        //when
        //shutdown 4 servers, leaving 4 remaining and therefore not a quorum.
        followers.call().subList( 0, 4 ).forEach( CoreClusterMember::shutdown );

        //then
        RaftMachine raft = leader.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class );
        assertEventually( "Leader should have stepped down.", raft::isLeader, FALSE, 2, TimeUnit.MINUTES );
    }
}
