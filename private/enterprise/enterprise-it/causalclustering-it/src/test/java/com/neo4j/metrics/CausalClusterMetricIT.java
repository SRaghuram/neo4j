/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.metrics.MetricsTestHelper.TimerField;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static com.neo4j.metrics.MetricsTestHelper.readTimerDoubleValue;
import static com.neo4j.metrics.MetricsTestHelper.readTimerLongValueAndAssert;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class CausalClusterMetricIT
{
    private static final int TIMEOUT = 15;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void startCluster() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 1 )
                .withSharedCoreParam( MetricsSettings.metricsEnabled, TRUE )
                .withSharedReadReplicaParam( MetricsSettings.metricsEnabled, TRUE )
                .withSharedCoreParam( MetricsSettings.csvEnabled, TRUE )
                .withSharedReadReplicaParam( MetricsSettings.csvEnabled, TRUE )
                .withSharedCoreParam( MetricsSettings.csvInterval, "100ms" )
                .withSharedReadReplicaParam( MetricsSettings.csvInterval, "100ms" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldMonitorRaftMessageDelay() throws Throwable
    {
        var leader = cluster.awaitLeader();

        var databaseNames = leader.managementService().listDatabases();
        assertThat( databaseNames, not( empty() ) );

        for ( var databaseName : databaseNames )
        {
            assertEventually( "message delay eventually recorded",
                    () -> readLongGaugeValue( metricsFile( leader, databaseName, "core.message_processing_delay" ) ),
                    greaterThanOrEqualTo( 0L ), TIMEOUT, SECONDS );

            assertEventually( "message timer count eventually recorded",
                    () -> readTimerLongValueAndAssert( metricsFile( leader, databaseName, "core.message_processing_timer" ),
                            ( newValue, currentValue ) -> newValue >= currentValue, TimerField.COUNT ), greaterThan( 0L ), TIMEOUT, SECONDS );

            assertEventually( "message timer max eventually recorded",
                    () -> readTimerDoubleValue( metricsFile( leader, databaseName, "core.message_processing_timer" ), TimerField.MAX ),
                    greaterThanOrEqualTo( 0d ), TIMEOUT, SECONDS );
        }
    }

    @Test
    void shouldMonitorCausalCluster() throws Exception
    {
        // when
        var coreMember = cluster.coreTx( ( db, tx ) ->
        {
            var node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // then
        for ( var db : cluster.coreMembers() )
        {
            assertAllNodesVisible( db.defaultDatabase() );
        }

        for ( var db : cluster.readReplicas() )
        {
            assertAllNodesVisible( db.defaultDatabase() );
        }

        assertEventually( "append index eventually accurate",
                () -> readLongGaugeValue( metricsFile( coreMember, "core.append_index" ) ),
                greaterThan( 0L ), TIMEOUT, SECONDS );

        assertEventually( "commit index eventually accurate",
                () -> readLongGaugeValue( metricsFile( coreMember, "core.commit_index" ) ),
                greaterThan( 0L ), TIMEOUT, SECONDS );

        assertEventually( "term eventually accurate",
                () -> readLongGaugeValue( metricsFile( coreMember, "core.term" ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, SECONDS );

        assertEventually( "tx pull requests received eventually accurate", () ->
        {
            long total = 0;
            for ( var member : cluster.coreMembers() )
            {
                total += readLongCounterValue( metricsFile( member, "catchup.tx_pull_requests_received" ) );
            }
            return total;
        }, greaterThan( 0L ), TIMEOUT, SECONDS );

        assertEventually( "tx retries eventually accurate",
                () -> readLongCounterValue( metricsFile( coreMember, "core.tx_retries" ) ), equalTo( 0L ),
                TIMEOUT, SECONDS );

        assertEventually( "is leader eventually accurate",
                () -> readLongGaugeValue( metricsFile( coreMember, "core.is_leader" ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, SECONDS );

        var readReplica = cluster.getReadReplicaById( 0 );

        assertEventually( "pull update request registered",
                () -> readLongCounterValue( metricsFile( readReplica, "read_replica.pull_updates" ) ),
                greaterThan( 0L ), TIMEOUT, SECONDS );

        assertEventually( "pull update request registered",
                () -> readLongCounterValue( metricsFile( readReplica, "read_replica.pull_update_highest_tx_id_requested" ) ),
                greaterThan( 0L ), TIMEOUT, SECONDS );

        assertEventually( "pull update response received",
                () -> readLongCounterValue( metricsFile( readReplica, "read_replica.pull_update_highest_tx_id_received" ) ),
                greaterThan( 0L ), TIMEOUT, SECONDS );
    }

    private static File metricsFile( ClusterMember member, String metricName ) throws InterruptedException
    {
        return metricsFile( member, member.defaultDatabase().databaseName(), metricName );
    }

    private static File metricsFile( ClusterMember member, String databaseName, String metricName ) throws InterruptedException
    {
        var metricsDir = new File( member.homeDir(), MetricsSettings.csvPath.defaultValue().toString() );
        var metric = "neo4j." + databaseName + ".causal_clustering." + metricName;
        return metricsCsv( metricsDir, metric );
    }

    private static void assertAllNodesVisible( GraphDatabaseAPI db ) throws Exception
    {
        try ( var tx = db.beginTx() )
        {
            ThrowingSupplier<Long,Exception> nodeCount = () -> count( tx.getAllNodes() );

            var config = db.getDependencyResolver().resolveDependency( Config.class );

            assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount, greaterThan( 0L ), TIMEOUT, SECONDS );

            for ( var node : tx.getAllNodes() )
            {
                assertEquals( "baz_bat", node.getProperty( "foobar" ) );
            }

            tx.commit();
        }
    }
}
