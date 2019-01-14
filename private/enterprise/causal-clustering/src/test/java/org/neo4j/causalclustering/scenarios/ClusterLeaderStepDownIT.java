/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.stream.Collectors.toList;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ClusterLeaderStepDownIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 8 ).withNumberOfReadReplicas( 0 );

    @Test
    public void leaderShouldStepDownWhenFollowersAreGone() throws Throwable
    {
        // when
        Cluster<?> cluster = clusterRule.startCluster();

        //Do some work to make sure the cluster is operating normally.
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( Label.label( "bam" ) );
            node.setProperty( "bam", "bam" );
            tx.success();
        } );

        ThrowingSupplier<List<CoreClusterMember>,Exception> followers = () -> cluster.coreMembers().stream().filter(
                m -> m.raft().currentRole() != Role.LEADER ).collect( toList() );
        assertEventually( "All followers visible", followers, Matchers.hasSize( 7 ), 2, TimeUnit.MINUTES );

        //when
        //shutdown 4 servers, leaving 4 remaining and therefore not a quorum.
        followers.get().subList( 0, 4 ).forEach( CoreClusterMember::shutdown );

        //then
        assertEventually( "Leader should have stepped down.", () -> leader.raft().isLeader(), Matchers.is( false ), 2,
                TimeUnit.MINUTES );
    }
}
