/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Read Replica Metrics" )
public class ReadReplicaMetrics extends LifecycleAdapter
{
    @Documented( "The total number of pull requests made by this instance" )
    private final String pullUpdates;
    @Documented( "The highest transaction id requested in a pull update by this instance" )
    private final String pullUpdateHighestTxIdRequested;
    @Documented( "The highest transaction id that has been pulled in the last pull updates by this instance" )
    private final String pullUpdateHighestTxIdReceived;

    private Monitors monitors;
    private MetricRegistry registry;

    private final PullRequestMetric pullRequestMetric = new PullRequestMetric();

    public ReadReplicaMetrics( String metricsPrefix, Monitors monitors, MetricRegistry registry )
    {
        this.pullUpdates = name( metricsPrefix, "causal_clustering.read_replica", "pull_updates" );
        this.pullUpdateHighestTxIdRequested = name( metricsPrefix,
                "causal_clustering.read_replica", "pull_update_highest_tx_id_requested" );
        this.pullUpdateHighestTxIdReceived = name( metricsPrefix,
                "causal_clustering.read_replica", "pull_update_highest_tx_id_received" );
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( pullRequestMetric );

        registry.register( pullUpdates, new MetricsCounter( pullRequestMetric::numberOfRequests ) );
        registry.register( pullUpdateHighestTxIdRequested, new MetricsCounter(  pullRequestMetric::lastRequestedTxId ) );
        registry.register( pullUpdateHighestTxIdReceived, new MetricsCounter(  pullRequestMetric::lastReceivedTxId ) );
    }

    @Override
    public void stop()
    {
        registry.remove( pullUpdates );
        registry.remove( pullUpdateHighestTxIdRequested );
        registry.remove( pullUpdateHighestTxIdReceived );

        monitors.removeMonitorListener( pullRequestMetric );
    }
}
