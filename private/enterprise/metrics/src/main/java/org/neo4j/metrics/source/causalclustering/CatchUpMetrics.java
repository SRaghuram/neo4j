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

@Documented( ".CatchUp Metrics" )
public class CatchUpMetrics extends LifecycleAdapter
{
    @Documented( "TX pull requests received from read replicas" )
    private final String txPullRequestsReceived;

    private final Monitors monitors;
    private final MetricRegistry registry;
    private final TxPullRequestsMetric txPullRequestsMetric = new TxPullRequestsMetric();

    public CatchUpMetrics( String metricsPrefix, Monitors monitors, MetricRegistry registry )
    {
        this.txPullRequestsReceived = name( metricsPrefix, "causal_clustering.catchup.tx_pull_requests_received" );
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( txPullRequestsMetric );
        registry.register( txPullRequestsReceived, new MetricsCounter( txPullRequestsMetric::txPullRequestsReceived ) );
    }

    @Override
    public void stop()
    {
        registry.remove( txPullRequestsReceived );
        monitors.removeMonitorListener( txPullRequestsMetric );
    }
}
