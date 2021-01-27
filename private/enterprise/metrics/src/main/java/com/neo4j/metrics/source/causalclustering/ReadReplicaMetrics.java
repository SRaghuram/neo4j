/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;
import com.neo4j.metrics.source.MetricGroup;
import com.neo4j.metrics.source.Metrics;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@ServiceProvider
@Documented( ".Read Replica Metrics" )
public class ReadReplicaMetrics extends Metrics
{
    private static final String CAUSAL_CLUSTERING_PREFIX = "causal_clustering.read_replica";

    @Documented( "The total number of pull requests made by this instance. (counter)" )
    private static final String PULL_UPDATES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "pull_updates" );
    @Documented( "The highest transaction id requested in a pull update by this instance. (counter)" )
    private static final String PULL_UPDATE_HIGHEST_TX_ID_REQUESTED_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX,
            "pull_update_highest_tx_id_requested" );
    @Documented( "The highest transaction id that has been pulled in the last pull updates by this instance. (counter)" )
    private static final String PULL_UPDATE_HIGHEST_TX_ID_RECEIVED_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX,
            "pull_update_highest_tx_id_received" );

    private final String pullUpdates;
    private final String pullUpdateHighestTxIdRequested;
    private final String pullUpdateHighestTxIdReceived;

    private final Monitors monitors;
    private final MetricsRegister registry;

    private final PullRequestMetric pullRequestMetric = new PullRequestMetric();

    /**
     * Only for generating documentation. The metrics documentation is generated through
     * service loading which requires a zero-argument constructor.
     */
    public ReadReplicaMetrics()
    {
        this( "", null, null );
    }

    public ReadReplicaMetrics( String metricsPrefix, Monitors monitors, MetricsRegister registry )
    {
        super( MetricGroup.CAUSAL_CLUSTERING );
        this.pullUpdates = name( metricsPrefix, PULL_UPDATES_TEMPLATE );
        this.pullUpdateHighestTxIdRequested = name( metricsPrefix, PULL_UPDATE_HIGHEST_TX_ID_REQUESTED_TEMPLATE );
        this.pullUpdateHighestTxIdReceived = name( metricsPrefix, PULL_UPDATE_HIGHEST_TX_ID_RECEIVED_TEMPLATE );
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( pullRequestMetric );

        registry.register( pullUpdates, () -> new MetricsCounter( pullRequestMetric::numberOfRequests ) );
        registry.register( pullUpdateHighestTxIdRequested, () -> new MetricsCounter(  pullRequestMetric::lastRequestedTxId ) );
        registry.register( pullUpdateHighestTxIdReceived, () -> new MetricsCounter(  pullRequestMetric::lastReceivedTxId ) );
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
