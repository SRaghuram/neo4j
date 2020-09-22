/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".CatchUp Metrics" )
public class CatchUpMetrics extends LifecycleAdapter
{
    @Documented( "TX pull requests received from read replicas. (counter)" )
    private static final String TX_PULL_REQUESTS_RECEIVED_TEMPLATE = name( "causal_clustering.catchup.tx_pull_requests_received" );

    private final String txPullRequestsReceived;

    private final Monitors monitors;
    private final MetricsRegister registry;
    private final TxPullRequestsMetric txPullRequestsMetric = new TxPullRequestsMetric();

    public CatchUpMetrics( String metricsPrefix, Monitors monitors, MetricsRegister registry )
    {
        this.txPullRequestsReceived = name( metricsPrefix, TX_PULL_REQUESTS_RECEIVED_TEMPLATE );
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( txPullRequestsMetric );
        registry.register( txPullRequestsReceived, () -> new MetricsCounter( txPullRequestsMetric::txPullRequestsReceived ) );
    }

    @Override
    public void stop()
    {
        registry.remove( txPullRequestsReceived );
        monitors.removeMonitorListener( txPullRequestsMetric );
    }
}
