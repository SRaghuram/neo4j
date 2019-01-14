/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

@Documented( ".CatchUp Metrics" )
public class CatchUpMetrics extends LifecycleAdapter
{
    private static final String CAUSAL_CLUSTERING_PREFIX = "neo4j.causal_clustering.catchup";

    @Documented( "TX pull requests received from read replicas" )
    public static final String TX_PULL_REQUESTS_RECEIVED = name( CAUSAL_CLUSTERING_PREFIX, "tx_pull_requests_received" );

    private Monitors monitors;
    private MetricRegistry registry;
    private final TxPullRequestsMetric txPullRequestsMetric = new TxPullRequestsMetric();

    public CatchUpMetrics( Monitors monitors, MetricRegistry registry )
    {
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( txPullRequestsMetric );
        registry.register( TX_PULL_REQUESTS_RECEIVED, new MetricsCounter( txPullRequestsMetric::txPullRequestsReceived ) );
    }

    @Override
    public void stop()
    {
        registry.remove( TX_PULL_REQUESTS_RECEIVED );
        monitors.removeMonitorListener( txPullRequestsMetric );
    }
}
