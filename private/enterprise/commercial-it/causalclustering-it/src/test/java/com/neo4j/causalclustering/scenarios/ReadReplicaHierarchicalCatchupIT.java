/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.catchup.CatchupServerProvider;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

import org.neo4j.internal.helpers.collection.Pair;

import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.graphdb.Label.label;

public class ReadReplicaHierarchicalCatchupIT
{
    private static final Map<Integer,String> serverGroups = Map.of( 0, "NORTH", 1, "NORTH", 2, "NORTH", 3, "EAST", 5, "EAST", 4, "WEST", 6, "WEST" );

    @Rule
    public ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
                    .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE )
                    .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, TRUE );

    @Test
    public void shouldCatchupThroughHierarchy() throws Throwable
    {
        clusterRule = clusterRule
                .withInstanceReadReplicaParam( CausalClusteringSettings.server_groups, serverGroups::get )
                .withInstanceCoreParam( CausalClusteringSettings.server_groups, serverGroups::get );

        // given
        Cluster cluster = clusterRule.startCluster();
        int numberOfNodesToCreate = 100;

        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.commit();
        } );

        // 0, 1, 2 are core instances
        DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodesToCreate, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        // 3, 4 are other DCs
        ReadReplica east3 = cluster.addReadReplicaWithId( 3 );
        east3.start();
        ReadReplica west4 = cluster.addReadReplicaWithId( 4 );
        west4.start();

        ReadReplicaToReadReplicaCatchupIT.checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            coreClusterMember.defaultDatabase().getDependencyResolver().resolveDependency( CatchupServerProvider.class ).catchupServer().stop();
        }

        // 5, 6 are other DCs
        ReadReplica east5 = cluster.addReadReplicaWithId( 5 );
        east5.setUpstreamDatabaseSelectionStrategy( "connect-randomly-within-server-group" );
        east5.start();
        ReadReplica west6 = cluster.addReadReplicaWithId( 6 );
        west6.setUpstreamDatabaseSelectionStrategy( "connect-randomly-within-server-group" );
        west6.start();

        ReadReplicaToReadReplicaCatchupIT.checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );

    }
}
