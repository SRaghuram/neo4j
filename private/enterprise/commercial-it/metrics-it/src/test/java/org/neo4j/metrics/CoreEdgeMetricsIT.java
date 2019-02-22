/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.readreplica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static org.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CoreEdgeMetricsIT
{
    private static final int TIMEOUT = 15;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 1 )
            .withSharedCoreParam( MetricsSettings.metricsEnabled, Settings.TRUE )
            .withSharedReadReplicaParam( MetricsSettings.metricsEnabled, Settings.TRUE )
            .withSharedCoreParam( MetricsSettings.csvEnabled, Settings.TRUE )
            .withSharedReadReplicaParam( MetricsSettings.csvEnabled, Settings.TRUE )
            .withSharedCoreParam( MetricsSettings.csvInterval, "100ms" )
            .withSharedReadReplicaParam( MetricsSettings.csvInterval, "100ms" );

    private Cluster<?> cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldMonitorCoreEdge() throws Exception
    {
        // given
        cluster = clusterRule.startCluster();

        // when
        CoreClusterMember coreMember = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            assertAllNodesVisible( db.database() );
        }

        for ( ReadReplica db : cluster.readReplicas() )
        {
            assertAllNodesVisible( db.database() );
        }

        File coreMetricsDir = new File( coreMember.homeDir(), csvPath.getDefaultValue() );

        assertEventually( "append index eventually accurate",
                () -> readLongGaugeValue( metricsCsv( coreMetricsDir, "neo4j.causal_clustering.core.append_index" ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "commit index eventually accurate",
                () -> readLongGaugeValue( metricsCsv( coreMetricsDir, "neo4j.causal_clustering.core.commit_index" ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "term eventually accurate",
                () -> readLongGaugeValue( metricsCsv( coreMetricsDir, "neo4j.causal_clustering.core.term" ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "tx pull requests received eventually accurate", () ->
        {
            long total = 0;
            for ( final File homeDir : cluster.coreMembers().stream().map( CoreClusterMember::homeDir ).collect( Collectors.toList()) )
            {
                File metricsDir = new File( homeDir, "metrics" );
                total += readLongCounterValue( metricsCsv( metricsDir, "neo4j.causal_clustering.catchup.tx_pull_requests_received" ) );
            }
            return total;
        }, greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "tx retries eventually accurate",
                () -> readLongCounterValue( metricsCsv( coreMetricsDir, "neo4j.causal_clustering.core.tx_retries" ) ), equalTo( 0L ),
                TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "is leader eventually accurate",
                () -> readLongGaugeValue( metricsCsv( coreMetricsDir, "neo4j.causal_clustering.core.is_leader" ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, TimeUnit.SECONDS );

        File readReplicaMetricsDir = new File( cluster.getReadReplicaById( 0 ).homeDir(), "metrics" );

        assertEventually( "pull update request registered",
                () -> readLongCounterValue( metricsCsv( readReplicaMetricsDir, "neo4j.causal_clustering.read_replica.pull_updates" ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "pull update request registered",
                () -> readLongCounterValue( metricsCsv( readReplicaMetricsDir, "neo4j.causal_clustering.read_replica.pull_update_highest_tx_id_requested" ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "pull update response received",
                () -> readLongCounterValue( metricsCsv( readReplicaMetricsDir, "neo4j.causal_clustering.read_replica.pull_update_highest_tx_id_received" ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );
    }

    private void assertAllNodesVisible( GraphDatabaseAPI db ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

            Config config = db.getDependencyResolver().resolveDependency( Config.class );

            assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount, greaterThan( 0L ), TIMEOUT, SECONDS );

            for ( Node node : db.getAllNodes() )
            {
                assertEquals( "baz_bat", node.getProperty( "foobar" ) );
            }

            tx.success();
        }
    }
}
