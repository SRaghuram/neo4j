/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;
import org.neo4j.metrics.output.EventReporter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Collections.emptySortedMap;

@Documented( ".Database log rotation metrics" )
public class LogRotationMetrics extends LifecycleAdapter
{
    private static final String LOG_ROTATION_PREFIX = "log_rotation";

    @Documented( "The total number of transaction log rotations executed so far." )
    private static final String LOG_ROTATION_EVENTS_TEMPLATE = name( LOG_ROTATION_PREFIX, "events" );
    @Documented( "The total time spent in rotating transaction logs so far." )
    private static final String LOG_ROTATION_TOTAL_TIME_TEMPLATE = name( LOG_ROTATION_PREFIX, "total_time" );
    @Documented( "The duration of the log rotation event." )
    private static final String LOG_ROTATION_DURATION_TEMPLATE = name( LOG_ROTATION_PREFIX, "duration" );

    private final String logRotationEvents;
    private final String logRotationTotalTime;
    private final String logRotationDuration;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final LogRotationMonitor logRotationMonitor;
    private final LogRotationMonitor listener;

    public LogRotationMetrics( String metricsPrefix, EventReporter reporter, MetricRegistry registry,
            Monitors monitors, LogRotationMonitor logRotationMonitor, JobScheduler jobScheduler )
    {
        this.logRotationEvents = name( metricsPrefix, LOG_ROTATION_EVENTS_TEMPLATE );
        this.logRotationTotalTime = name( metricsPrefix, LOG_ROTATION_TOTAL_TIME_TEMPLATE );
        this.logRotationDuration = name( metricsPrefix, LOG_ROTATION_DURATION_TEMPLATE );
        this.registry = registry;
        this.monitors = monitors;
        this.logRotationMonitor = logRotationMonitor;
        this.listener = new ReportingLogRotationMonitor( reporter, jobScheduler, logRotationDuration );
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( listener );

        registry.register( logRotationEvents, new MetricsCounter( logRotationMonitor::numberOfLogRotations ) );
        registry.register( logRotationTotalTime, new MetricsCounter( logRotationMonitor::logRotationAccumulatedTotalTimeMillis ) );
    }

    @Override
    public void stop()
    {
        monitors.removeMonitorListener( listener );

        registry.remove( logRotationEvents );
        registry.remove( logRotationTotalTime );
    }

    private static final class ReportingLogRotationMonitor extends LogRotationMonitorAdapter
    {
        private final EventReporter reporter;
        private final JobScheduler scheduler;
        private final String metricName;

        ReportingLogRotationMonitor( EventReporter reporter, JobScheduler scheduler, String metricName )
        {
            this.reporter = reporter;
            this.scheduler = scheduler;
            this.metricName = metricName;
        }

        @Override
        public void finishLogRotation( long currentLogVersion, long rotationMillis )
        {
            scheduler.schedule( Group.METRICS_EVENT, () ->
            {
                SortedMap<String,Gauge> gauges = new TreeMap<>();
                gauges.put( metricName, () -> rotationMillis );
                reporter.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
            } );
        }
    }
}
