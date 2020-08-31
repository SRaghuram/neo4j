/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.configuration.MetricsSettings;
import com.neo4j.metrics.MetricsTestHelper.TimerField;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static com.neo4j.configuration.CausalClusteringSettings.raft_advertised_address;
import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static com.neo4j.metrics.MetricsTestHelper.readTimerDoubleValue;
import static com.neo4j.metrics.MetricsTestHelper.readTimerLongValueAndAssert;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.condition;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
class CausalClusterMetricIT
{
    private static final int TIMEOUT = 60;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;
    private int noCoreMembers;

    @BeforeAll
    void startCluster() throws Exception
    {
        noCoreMembers = 3;
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( noCoreMembers )
                .withNumberOfReadReplicas( 1 )
                .withSharedCoreParam( MetricsSettings.metrics_enabled, TRUE )
                .withSharedReadReplicaParam( MetricsSettings.metrics_enabled, TRUE )
                .withSharedCoreParam( MetricsSettings.csv_enabled, TRUE )
                .withSharedReadReplicaParam( MetricsSettings.csv_enabled, TRUE )
                .withSharedCoreParam( MetricsSettings.csv_interval, "100ms" )
                .withSharedReadReplicaParam( MetricsSettings.csv_interval, "100ms" );

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
                    value -> value >= 0L, TIMEOUT, SECONDS );

            assertEventually( "message timer count eventually recorded",
                    () -> readTimerLongValueAndAssert( metricsFile( leader, databaseName, "core.message_processing_timer" ),
                            ( newValue, currentValue ) -> newValue >= currentValue, TimerField.COUNT ), value -> value > 0L, TIMEOUT, SECONDS );

            assertEventually( "message timer max eventually recorded",
                    () -> readTimerDoubleValue( metricsFile( leader, databaseName, "core.message_processing_timer" ), TimerField.MAX ),
                    value -> value >= 0d, TIMEOUT, SECONDS );
        }
    }

    @Test
    void shouldMonitorAkkaDiscovery() throws Exception
    {
        // when
        var coreMember = cluster.awaitLeader();

        // then
        assertEventually( "convergence",
                () -> readLongGaugeValue( discoveryMetricsFile( coreMember, "core.discovery.cluster.converged" ) ),
                value -> value == 1L, TIMEOUT, SECONDS );

        assertEventually( "members eventually accurate",
                () -> readLongGaugeValue( discoveryMetricsFile( coreMember, "core.discovery.cluster.members" ) ),
                value -> value == noCoreMembers, TIMEOUT, SECONDS );

        assertEventually( "replicated data size accurate",
                () -> readLongGaugeValue( discoveryMetricsFile( coreMember, "core.discovery.replicated_data.member_data.visible" ) ),
                value -> value >= noCoreMembers, TIMEOUT, SECONDS );

        assertEventually( "replicated data size vvector accurate",
                () -> readLongGaugeValue( discoveryMetricsFile( coreMember, "core.discovery.replicated_data.member_data.visible" ) ),
                value -> value >= noCoreMembers, TIMEOUT, SECONDS );
    }

    @Test
    void shouldMonitorRaft() throws Exception
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
                () -> readLongGaugeValue( raftMetricsFile( coreMember, "core.append_index" ) ), value -> value > 0L, TIMEOUT, SECONDS );

        assertEventually( "commit index eventually accurate",
                () -> readLongGaugeValue( raftMetricsFile( coreMember, "core.commit_index" ) ), value -> value > 0L, TIMEOUT, SECONDS );

        assertEventually( "applied index eventually accurate",
                () -> readLongGaugeValue( raftMetricsFile( coreMember, "core.applied_index" ) ), value -> value > 0L, TIMEOUT, SECONDS );

        assertEventually( "term eventually accurate",
                () -> readLongGaugeValue( raftMetricsFile( coreMember, "core.term" ) ), value -> value >= 0L, TIMEOUT, SECONDS );

        assertEventually( "tx pull requests received eventually accurate", () ->
        {
            long total = 0;
            for ( var member : cluster.coreMembers() )
            {
                total += readLongCounterValue( raftMetricsFile( member, "catchup.tx_pull_requests_received" ) );
            }
            return total;
        }, value -> value > 0L, TIMEOUT, SECONDS );

        assertEventually( "tx retries eventually accurate",
                () -> readLongCounterValue( raftMetricsFile( coreMember, "core.tx_retries" ) ), equalityCondition( 0L ),
                TIMEOUT, SECONDS );

        assertEventually( "is leader eventually accurate",
                () -> readLongGaugeValue( raftMetricsFile( coreMember, "core.is_leader" ) ),
                value -> isMemberLeader( coreMember ) ? (value == 1) : (value == 0), TIMEOUT, SECONDS );

        assertEventually( "is last message from leader elapsed time eventually accurate",
                () -> readLongGaugeValue( raftMetricsFile( coreMember, "core.last_leader_message" ) ),
                value -> isMemberLeader( coreMember ) ? (value == 0) : (value > 0), TIMEOUT, SECONDS );

        var readReplica = cluster.getReadReplicaByIndex( 0 );

        assertEventually( "pull update request registered",
                () -> readLongCounterValue( raftMetricsFile( readReplica, "read_replica.pull_updates" ) ), value -> value > 0L, TIMEOUT, SECONDS );

        assertEventually( "pull update request registered",
                () -> readLongCounterValue( raftMetricsFile( readReplica, "read_replica.pull_update_highest_tx_id_requested" ) ),
                value -> value > 0L, TIMEOUT, SECONDS );

        assertEventually( "pull update response received",
                () -> readLongCounterValue( raftMetricsFile( readReplica, "read_replica.pull_update_highest_tx_id_received" ) ),
                value -> value > 0L, TIMEOUT, SECONDS );
    }

    private boolean isMemberLeader( ClusterMember member )
    {
        var dependencyResolver = member.defaultDatabase().getDependencyResolver();
        return dependencyResolver.resolveDependency( RoleProvider.class ).currentRole() == Role.LEADER;
    }

    private static Path discoveryMetricsFile( ClusterMember member, String metricName )
    {
        return metricsFile( member, null, metricName );
    }

    private static Path raftMetricsFile( ClusterMember member, String metricName )
    {
        return metricsFile( member, member.defaultDatabase().databaseName(), metricName );
    }

    private static Path metricsFile( ClusterMember member, String databaseName, String metricName )
    {
        var metricsDir = member.homePath().resolve( MetricsSettings.csv_path.defaultValue().toString() );
        var metric = MetricRegistry.name( "neo4j", databaseName, "causal_clustering", metricName );
        return metricsCsv( metricsDir, metric );
    }

    private static void assertAllNodesVisible( GraphDatabaseAPI db )
    {
        try ( var tx = db.beginTx() )
        {
            Callable<Long> nodeCount = () -> count( tx.getAllNodes() );

            var config = db.getDependencyResolver().resolveDependency( Config.class );

            assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount, condition( value -> value > 0L ),
                    TIMEOUT, SECONDS );

            for ( var node : tx.getAllNodes() )
            {
                assertEquals( "baz_bat", node.getProperty( "foobar" ) );
            }

            tx.commit();
        }
    }
}
