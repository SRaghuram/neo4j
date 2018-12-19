/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.cypher.PlanCacheMetricsMonitor;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Cypher metrics" )
public class CypherMetrics extends LifecycleAdapter
{
    private static final String CYPHER_PREFIX = "cypher";

    @Documented( "The total number of times Cypher has decided to re-plan a query" )
    private final String replanEvents;

    @Documented( "The total number of seconds waited between query replans" )
    private final String replanWaitTime;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final PlanCacheMetricsMonitor cacheMonitor = new PlanCacheMetricsMonitor();

    public CypherMetrics( String metricsPrefix, MetricRegistry registry, Monitors monitors )
    {
        this.replanEvents = name( metricsPrefix, CYPHER_PREFIX, "replan_events" );
        this.replanWaitTime = name( metricsPrefix, CYPHER_PREFIX, "replan_wait_time" );
        this.registry = registry;
        this.monitors = monitors;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( cacheMonitor );
        registry.register( replanEvents, new MetricsCounter( cacheMonitor::numberOfReplans ) );
        registry.register( replanWaitTime, new MetricsCounter( cacheMonitor::replanWaitTime ) );
    }

    @Override
    public void stop()
    {
        registry.remove( replanEvents );
        registry.remove( replanWaitTime );
        monitors.removeMonitorListener( cacheMonitor );
    }
}

