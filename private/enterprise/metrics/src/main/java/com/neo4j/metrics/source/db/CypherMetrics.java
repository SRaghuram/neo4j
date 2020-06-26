/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.metrics.metric.MetricsCounter;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.cypher.PlanCacheMetricsMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Cypher metrics" )
public class CypherMetrics extends LifecycleAdapter
{
    private static final String CYPHER_PREFIX = "cypher";

    @Documented( "The total number of times Cypher has decided to re-plan a query. (counter)" )
    private static final String REPLAN_EVENTS_TEMPLATE = name( CYPHER_PREFIX, "replan_events" );
    @Documented( "The total number of seconds waited between query replans. (counter)" )
    private static final String REPLAN_WAIT_TIME_TEMPLATE = name( CYPHER_PREFIX, "replan_wait_time" );

    private final String replanEvents;
    private final String replanWaitTime;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final PlanCacheMetricsMonitor cacheMonitor = new PlanCacheMetricsMonitor();

    public CypherMetrics( String metricsPrefix, MetricRegistry registry, Monitors monitors )
    {
        this.replanEvents = name( metricsPrefix, REPLAN_EVENTS_TEMPLATE );
        this.replanWaitTime = name( metricsPrefix, REPLAN_WAIT_TIME_TEMPLATE );
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

