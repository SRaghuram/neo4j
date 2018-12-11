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
    private static final String NAME_PREFIX = "neo4j.cypher";

    @Documented( "The total number of times Cypher has decided to re-plan a query" )
    public static final String REPLAN_EVENTS = name( NAME_PREFIX, "replan_events" );

    @Documented( "The total number of seconds waited between query replans" )
    public static final String REPLAN_WAIT_TIME = name( NAME_PREFIX, "replan_wait_time" );

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final PlanCacheMetricsMonitor cacheMonitor = new PlanCacheMetricsMonitor();

    public CypherMetrics( MetricRegistry registry, Monitors monitors )
    {
        this.registry = registry;
        this.monitors = monitors;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( cacheMonitor );
        registry.register( REPLAN_EVENTS, new MetricsCounter( cacheMonitor::numberOfReplans ) );
        registry.register( REPLAN_WAIT_TIME, new MetricsCounter( cacheMonitor::replanWaitTime ) );
    }

    @Override
    public void stop()
    {
        registry.remove( REPLAN_EVENTS );
        registry.remove( REPLAN_WAIT_TIME );
        monitors.removeMonitorListener( cacheMonitor );
    }
}

