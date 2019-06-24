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

import java.util.UUID;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.causalclustering.read_replica.SpecificReplicaStrategy.upstreamFactory;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReadReplicaToReadReplicaCatchupIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
                    .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE )
                    .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, TRUE );

    @Test
    public void shouldEventuallyPullTransactionAcrossReadReplicas() throws Throwable
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        int numberOfNodesToCreate = 100;

        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.success();
        } );

        DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodesToCreate, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        ReadReplica firstReadReplica = cluster.addReadReplicaWithIdAndMonitors( 101, new Monitors() );

        firstReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            coreClusterMember.defaultDatabase().getDependencyResolver().resolveDependency( CatchupServerProvider.class ).catchupServer().stop();
        }

        // when
        upstreamFactory.setCurrent( firstReadReplica );
        ReadReplica secondReadReplica = cluster.addReadReplicaWithId( 202 );
        secondReadReplica.setUpstreamDatabaseSelectionStrategy( "specific" );

        secondReadReplica.start();

        // then

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );
    }

    @Test
    public void shouldCatchUpFromCoresWhenPreferredReadReplicasAreUnavailable() throws Throwable
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        int numberOfNodes = 1;
        int firstReadReplicaLocalMemberId = 101;

        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.success();
        } );

        DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        ReadReplica firstReadReplica =
                cluster.addReadReplicaWithIdAndMonitors( firstReadReplicaLocalMemberId, new Monitors() );

        firstReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes );

        upstreamFactory.setCurrent( firstReadReplica );

        ReadReplica secondReadReplica = cluster.addReadReplicaWithId( 202 );
        secondReadReplica.setUpstreamDatabaseSelectionStrategy( "specific" );

        secondReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes );

        firstReadReplica.shutdown();
        upstreamFactory.reset();

        cluster.removeReadReplicaWithMemberId( firstReadReplicaLocalMemberId );

        // when
        // More transactions into core
        DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        // then
        // reached second read replica from cores
        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes * 2 );
    }

    static void checkDataHasReplicatedToReadReplicas( Cluster cluster, long numberOfNodes ) throws Exception
    {
        for ( final ReadReplica server : cluster.readReplicas() )
        {
            GraphDatabaseService readReplica = server.defaultDatabase();
            try ( Transaction tx = readReplica.beginTx() )
            {
                ThrowingSupplier<Long,Exception> nodeCount = () -> count( readReplica.getAllNodes() );
                assertEventually( "node to appear on read replica", nodeCount, is( numberOfNodes ), 1, MINUTES );

                for ( Node node : readReplica.getAllNodes() )
                {
                    assertThat( node.getProperty( "foobar" ).toString(), startsWith( "baz_bat" ) );
                }

                tx.success();
            }
        }
    }
}
