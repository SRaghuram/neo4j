package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
public class LeaderTransferIT
{
    @Inject
    ClusterFactory clusterFactory;

    @Test
    void name() throws ExecutionException, InterruptedException, TimeoutException
    {
        var cluster = clusterFactory.createCluster(
                clusterConfig().withSharedCoreParam( CausalClusteringSettings.leadership_priority_groups, "prio" ).withNumberOfCoreMembers( 3 ) );
        cluster.start();
        cluster.awaitLeader();

        var additionalCore = cluster.addCoreMemberWithId( 3 );
        additionalCore.config().set( CausalClusteringSettings.server_groups, List.of( "prio" ) );

        additionalCore.start();

        System.out.println(additionalCore.id());

        assertEventually( cluster::awaitLeader, coreClusterMember -> coreClusterMember.id().equals( additionalCore.id() ), 3, TimeUnit.MINUTES );
    }
}
