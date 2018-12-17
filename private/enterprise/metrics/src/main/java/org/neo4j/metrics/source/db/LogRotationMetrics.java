/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.api.DefaultTransactionTracer;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;
import org.neo4j.metrics.output.EventReporter;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Collections.emptySortedMap;

@Documented( ".Database log rotation metrics" )
public class LogRotationMetrics extends LifecycleAdapter
{
    @Documented( "The total number of transaction log rotations executed so far" )
    private final String logRotationEvents;
    @Documented( "The total time spent in rotating transaction logs so far" )
    private final String logRotationTotalTime;
    @Documented( "The duration of the log rotation event" )
    private final String logRotationDuration;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final LogRotationMonitor logRotationMonitor;
    private final DefaultTransactionTracer.Monitor listener;

    public LogRotationMetrics( String metricsPrefix, EventReporter reporter, MetricRegistry registry,
            Monitors monitors, LogRotationMonitor logRotationMonitor )
    {
        this.logRotationEvents = name( metricsPrefix, "log_rotation.events" );
        this.logRotationTotalTime = name( metricsPrefix, "log_rotation.total_time" );
        this.logRotationDuration = name( metricsPrefix, "log_rotation.log_rotation_duration" );
        this.registry = registry;
        this.monitors = monitors;
        this.logRotationMonitor = logRotationMonitor;
        this.listener = durationMillis ->
        {
            final SortedMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( logRotationDuration, () -> durationMillis );
            reporter.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( listener );

        registry.register( logRotationEvents, new MetricsCounter( logRotationMonitor::numberOfLogRotationEvents ) );
        registry.register( logRotationTotalTime, new MetricsCounter( logRotationMonitor::logRotationAccumulatedTotalTimeMillis ) );
    }

    @Override
    public void stop()
    {
        monitors.removeMonitorListener( listener );

        registry.remove( logRotationEvents );
        registry.remove( logRotationTotalTime );
    }
}
