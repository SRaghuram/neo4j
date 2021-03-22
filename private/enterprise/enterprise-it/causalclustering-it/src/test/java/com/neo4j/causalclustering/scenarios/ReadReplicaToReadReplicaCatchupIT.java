/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.catchup.CatchupServerProvider;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.causalclustering.TestAllClusterTypes;
import org.junit.jupiter.api.TestInstance;

import java.util.UUID;
import java.util.concurrent.Callable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.read_replica.SpecificReplicaStrategy.upstreamFactory;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class ReadReplicaToReadReplicaCatchupIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    void start( ClusterConfig.ClusterType clusterType ) throws Exception
    {
        var clusterConfig = clusterConfig()
                .withClusterType( clusterType )
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedPrimaryParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
                .withSharedPrimaryParam( CausalClusteringSettings.multi_dc_license, TRUE )
                .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, TRUE );
        cluster = clusterFactory.start( clusterConfig );
    }

    @TestAllClusterTypes
    void shouldEventuallyPullTransactionAcrossReadReplicas( ClusterConfig.ClusterType type ) throws Throwable
    {
        start( type );
        // given
        int numberOfNodesToCreate = 100;

        cluster.primaryTx( ( db, tx ) ->
        {
            tx.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.commit();
        } );

        DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodesToCreate, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        ReadReplica firstReadReplica = cluster.addReadReplicaWithIndexAndMonitors( 101, new Monitors() );

        firstReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );

        for ( CoreClusterMember coreClusterMember : cluster.primaryMembers() )
        {
            coreClusterMember.defaultDatabase().getDependencyResolver().resolveDependency( CatchupServerProvider.class ).catchupServer().stop();
        }

        // when
        upstreamFactory.setCurrent( firstReadReplica );
        ReadReplica secondReadReplica = cluster.addReadReplicaWithIndex( 202 );
        secondReadReplica.setUpstreamDatabaseSelectionStrategy( "specific" );

        secondReadReplica.start();

        // then

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );
    }

    @TestAllClusterTypes
    void shouldCatchUpFromCoresWhenPreferredReadReplicasAreUnavailable( ClusterConfig.ClusterType type ) throws Throwable
    {
        start(type);

        // given
        int numberOfNodes = 1;
        int firstReadReplicaLocalIndex = 101;

        cluster.primaryTx( ( db, tx ) ->
        {
            tx.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.commit();
        } );

        DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        ReadReplica firstReadReplica =
                cluster.addReadReplicaWithIndexAndMonitors( firstReadReplicaLocalIndex, new Monitors() );

        firstReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes );

        upstreamFactory.setCurrent( firstReadReplica );

        ReadReplica secondReadReplica = cluster.addReadReplicaWithIndex( 202 );
        secondReadReplica.setUpstreamDatabaseSelectionStrategy( "specific" );

        secondReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes );

        firstReadReplica.shutdown();
        upstreamFactory.reset();

        cluster.removeReadReplicaWithIndex( firstReadReplicaLocalIndex );

        // when
        // More transactions into core
        DataCreator.createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        // then
        // reached second read replica from cores
        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes * 2 );
    }

    static void checkDataHasReplicatedToReadReplicas( Cluster cluster, long numberOfNodes )
    {
        for ( final ReadReplica server : cluster.readReplicas() )
        {
            GraphDatabaseService readReplica = server.defaultDatabase();
            try ( Transaction tx = readReplica.beginTx() )
            {
                Callable<Long> nodeCount = () -> count( tx.getAllNodes() );
                assertEventually( "node to appear on read replica", nodeCount, equalityCondition( numberOfNodes ), 1, MINUTES );

                for ( Node node : tx.getAllNodes() )
                {
                    assertThat( node.getProperty( "foobar" ).toString(), startsWith( "baz_bat" ) );
                }

                tx.commit();
            }
        }
    }
}
