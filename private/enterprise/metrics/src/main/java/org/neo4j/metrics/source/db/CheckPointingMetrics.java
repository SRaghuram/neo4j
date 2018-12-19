/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.TreeMap;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointDurationMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;
import org.neo4j.metrics.output.EventReporter;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Collections.emptySortedMap;

@Documented( ".Database checkpointing metrics" )
public class CheckPointingMetrics extends LifecycleAdapter
{
    private static final String CHECK_POINT_PREFIX = "check_point";

    @Documented( "The total number of check point events executed so far" )
    private final String checkPointEvents;
    @Documented( "The total time spent in check pointing so far" )
    private final String checkPointTotalTime;
    @Documented( "The duration of the check point event" )
    private final String checkPointDuration;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final CheckPointerMonitor checkPointerMonitor;
    private final CheckpointDurationMonitor listener;

    public CheckPointingMetrics( String metricsPrefix, EventReporter reporter, MetricRegistry registry,
            Monitors monitors, CheckPointerMonitor checkPointerMonitor )
    {
        this.checkPointEvents = name( metricsPrefix, CHECK_POINT_PREFIX,"events" );
        this.checkPointTotalTime = name( metricsPrefix, CHECK_POINT_PREFIX,"total_time" );
        this.checkPointDuration = name( metricsPrefix, CHECK_POINT_PREFIX, "check_point_duration" );
        this.registry = registry;
        this.monitors = monitors;
        this.checkPointerMonitor = checkPointerMonitor;
        this.listener = durationMillis ->
        {
            TreeMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( checkPointDuration, () -> durationMillis );
            reporter.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( listener );

        registry.register( checkPointEvents, new MetricsCounter( checkPointerMonitor::numberOfCheckPointEvents ) );
        registry.register( checkPointTotalTime, new MetricsCounter( checkPointerMonitor::checkPointAccumulatedTotalTimeMillis ) );
    }

    @Override
    public void stop()
    {
        monitors.removeMonitorListener( listener );

        registry.remove( checkPointEvents );
        registry.remove( checkPointTotalTime );
    }
}
