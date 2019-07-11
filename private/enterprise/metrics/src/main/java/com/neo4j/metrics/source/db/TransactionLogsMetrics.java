/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.output.EventReporter;

import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.impl.transaction.log.monitor.LogAppenderMonitor;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Collections.emptySortedMap;

@Documented( ".Database transaction log metrics" )
public class TransactionLogsMetrics extends LifecycleAdapter
{
    private static final String TX_LOG_PREFIX = "log";

    @Documented( "The total number of transaction log rotations executed so far." )
    private static final String LOG_ROTATION_EVENTS_TEMPLATE = name( TX_LOG_PREFIX, "rotation_events" );
    @Documented( "The total time spent in rotating transaction logs so far." )
    private static final String LOG_ROTATION_TOTAL_TIME_TEMPLATE = name( TX_LOG_PREFIX, "rotation_total_time" );
    @Documented( "The duration of the log rotation event." )
    private static final String LOG_ROTATION_DURATION_TEMPLATE = name( TX_LOG_PREFIX, "rotation_duration" );
    @Documented( "The total number of bytes appended to transaction log." )
    private static final String LOG_APPENDED_BYTES = name( TX_LOG_PREFIX, "appended_bytes" );

    private final String logRotationEvents;
    private final String logRotationTotalTime;
    private final String logRotationDuration;
    private final String logApppendedBytes;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final LogRotationMonitor logRotationMonitor;
    private final LogAppenderMonitor logAppenderMonitor;
    private final LogRotationMonitor listener;

    public TransactionLogsMetrics( String metricsPrefix, EventReporter reporter, MetricRegistry registry,
            Monitors monitors, LogRotationMonitor logRotationMonitor, LogAppenderMonitor logAppenderMonitor, JobScheduler jobScheduler )
    {
        this.logRotationEvents = name( metricsPrefix, LOG_ROTATION_EVENTS_TEMPLATE );
        this.logRotationTotalTime = name( metricsPrefix, LOG_ROTATION_TOTAL_TIME_TEMPLATE );
        this.logRotationDuration = name( metricsPrefix, LOG_ROTATION_DURATION_TEMPLATE );
        this.logApppendedBytes = name( metricsPrefix, LOG_APPENDED_BYTES );
        this.registry = registry;
        this.monitors = monitors;
        this.logRotationMonitor = logRotationMonitor;
        this.logAppenderMonitor = logAppenderMonitor;
        this.listener = new ReportingLogRotationMonitor( reporter, jobScheduler, logRotationDuration );
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( listener );

        registry.register( logRotationEvents, new MetricsCounter( logRotationMonitor::numberOfLogRotations ) );
        registry.register( logRotationTotalTime, new MetricsCounter( logRotationMonitor::logRotationAccumulatedTotalTimeMillis ) );
        registry.register( logApppendedBytes, new MetricsCounter( logAppenderMonitor::appendedBytes ) );
    }

    @Override
    public void stop()
    {
        monitors.removeMonitorListener( listener );

        registry.remove( logRotationEvents );
        registry.remove( logRotationTotalTime );
        registry.remove( logApppendedBytes );
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
