/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;
import com.neo4j.metrics.source.MetricGroup;
import com.neo4j.metrics.source.Metrics;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cypher.PlanCacheMetricsMonitor;
import org.neo4j.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@ServiceProvider
@Documented( ".Cypher metrics" )
public class CypherMetrics extends Metrics
{
    private static final String CYPHER_PREFIX = "cypher";

    @Documented( "The total number of times Cypher has decided to re-plan a query. (counter)" )
    private static final String REPLAN_EVENTS_TEMPLATE = name( CYPHER_PREFIX, "replan_events" );
    @Documented( "The total number of seconds waited between query replans. (counter)" )
    private static final String REPLAN_WAIT_TIME_TEMPLATE = name( CYPHER_PREFIX, "replan_wait_time" );

    private final String replanEvents;
    private final String replanWaitTime;

    private final MetricsRegister registry;
    private final Monitors monitors;
    private final PlanCacheMetricsMonitor cacheMonitor = new PlanCacheMetricsMonitor();

    /**
     * Only for generating documentation. The metrics documentation is generated through
     * service loading which requires a zero-argument constructor.
     */
    public CypherMetrics()
    {
        this( "", null, null );
    }

    public CypherMetrics( String metricsPrefix, MetricsRegister registry, Monitors monitors )
    {
        super( MetricGroup.GENERAL );
        this.replanEvents = name( metricsPrefix, REPLAN_EVENTS_TEMPLATE );
        this.replanWaitTime = name( metricsPrefix, REPLAN_WAIT_TIME_TEMPLATE );
        this.registry = registry;
        this.monitors = monitors;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( cacheMonitor );
        registry.register( replanEvents, () -> new MetricsCounter( cacheMonitor::numberOfReplans ) );
        registry.register( replanWaitTime, () -> new MetricsCounter( cacheMonitor::replanWaitTime ) );
    }

    @Override
    public void stop()
    {
        registry.remove( replanEvents );
        registry.remove( replanWaitTime );
        monitors.removeMonitorListener( cacheMonitor );
    }
}

