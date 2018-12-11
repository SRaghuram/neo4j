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
    private static final String CAUSAL_CLUSTERING_PREFIX = "neo4j.causal_clustering.read_replica";

    @Documented( "The total number of pull requests made by this instance" )
    public static final String PULL_UPDATES = name( CAUSAL_CLUSTERING_PREFIX, "pull_updates" );
    @Documented( "The highest transaction id requested in a pull update by this instance" )
    public static final String PULL_UPDATE_HIGHEST_TX_ID_REQUESTED = name( CAUSAL_CLUSTERING_PREFIX,
            "pull_update_highest_tx_id_requested" );
    @Documented( "The highest transaction id that has been pulled in the last pull updates by this instance" )
    public static final String PULL_UPDATE_HIGHEST_TX_ID_RECEIVED = name( CAUSAL_CLUSTERING_PREFIX,
            "pull_update_highest_tx_id_received" );

    private Monitors monitors;
    private MetricRegistry registry;

    private final PullRequestMetric pullRequestMetric = new PullRequestMetric();

    public ReadReplicaMetrics( Monitors monitors, MetricRegistry registry )
    {
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( pullRequestMetric );

        registry.register( PULL_UPDATES, new MetricsCounter( pullRequestMetric::numberOfRequests ) );
        registry.register( PULL_UPDATE_HIGHEST_TX_ID_REQUESTED, new MetricsCounter(  pullRequestMetric::lastRequestedTxId ) );
        registry.register( PULL_UPDATE_HIGHEST_TX_ID_RECEIVED, new MetricsCounter(  pullRequestMetric::lastReceivedTxId ) );
    }

    @Override
    public void stop()
    {
        registry.remove( PULL_UPDATES );
        registry.remove( PULL_UPDATE_HIGHEST_TX_ID_REQUESTED );
        registry.remove( PULL_UPDATE_HIGHEST_TX_ID_RECEIVED );

        monitors.removeMonitorListener( pullRequestMetric );
    }
}
