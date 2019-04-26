/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Settings;
import org.neo4j.metrics.MetricsTestHelper.TimerField;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static org.neo4j.metrics.MetricsTestHelper.readTimerDoubleValue;
import static org.neo4j.metrics.MetricsTestHelper.readTimerLongValueAndAssert;
import static org.neo4j.test.assertion.Assert.assertEventually;

@Ignore( "Have to make metrics multi-database capable" ) // TODO
public class RaftMessageProcessingMetricIT
{
    private static final int TIMEOUT = 15;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "1s" )
            .withSharedCoreParam( MetricsSettings.metricsEnabled, Settings.TRUE )
            .withSharedCoreParam( MetricsSettings.csvEnabled, Settings.TRUE )
            .withSharedCoreParam( MetricsSettings.csvInterval, "100ms" );

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldMonitorMessageDelay() throws Throwable
    {
        // given
        cluster = clusterRule.startCluster();

        // then
        CoreClusterMember leader = cluster.awaitLeader();
        File coreMetricsDir = new File( leader.homeDir(), MetricsSettings.csvPath.getDefaultValue() );

        assertEventually( "message delay eventually recorded",
                () -> readLongGaugeValue( metricsCsv( coreMetricsDir, "neo4j.causal_clustering.core.message_processing_delay" ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "message timer count eventually recorded",
                () -> readTimerLongValueAndAssert( metricsCsv( coreMetricsDir, "neo4j.causal_clustering.core.message_processing_timer" ),
                        ( newValue, currentValue ) -> newValue >= currentValue, TimerField.COUNT ), greaterThan( 0L ), TIMEOUT,
                TimeUnit.SECONDS );

        assertEventually( "message timer max eventually recorded",
                () -> readTimerDoubleValue( metricsCsv( coreMetricsDir, "neo4j.causal_clustering.core.message_processing_timer" ),
                        TimerField.MAX ), greaterThanOrEqualTo( 0d ), TIMEOUT, TimeUnit.SECONDS );
    }
}
