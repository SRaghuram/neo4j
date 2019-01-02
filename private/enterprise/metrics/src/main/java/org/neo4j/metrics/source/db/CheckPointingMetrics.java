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
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitorAdapter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;
import org.neo4j.metrics.output.EventReporter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Collections.emptySortedMap;

@Documented( ".Database checkpointing metrics" )
public class CheckPointingMetrics extends LifecycleAdapter
{
    private static final String CHECK_POINT_PREFIX = "check_point";

    @Documented( "The total number of check point events executed so far." )
    private static final String CHECK_POINT_EVENTS_TEMPLATE = name( CHECK_POINT_PREFIX, "events" );
    @Documented( "The total time spent in check pointing so far." )
    private static final String CHECK_POINT_TOTAL_TIME_TEMPLATE = name( CHECK_POINT_PREFIX, "total_time" );
    @Documented( "The duration of the check point event." )
    private static final String CHECK_POINT_DURATION_TEMPLATE = name( CHECK_POINT_PREFIX, "duration" );

    private final String checkPointEvents;
    private final String checkPointTotalTime;
    private final String checkPointDuration;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final CheckPointerMonitor checkPointerMonitor;
    private final CheckPointerMonitor listener;

    public CheckPointingMetrics( String metricsPrefix, EventReporter reporter, MetricRegistry registry, Monitors monitors,
            CheckPointerMonitor checkPointerMonitor, JobScheduler jobScheduler )
    {
        this.checkPointEvents = name( metricsPrefix, CHECK_POINT_EVENTS_TEMPLATE );
        this.checkPointTotalTime = name( metricsPrefix, CHECK_POINT_TOTAL_TIME_TEMPLATE );
        this.checkPointDuration = name( metricsPrefix, CHECK_POINT_DURATION_TEMPLATE );
        this.registry = registry;
        this.monitors = monitors;
        this.checkPointerMonitor = checkPointerMonitor;
        this.listener = new ReportingCheckPointerMonitor( reporter, jobScheduler, checkPointDuration );
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( listener );

        registry.register( checkPointEvents, new MetricsCounter( checkPointerMonitor::numberOfCheckPoints ) );
        registry.register( checkPointTotalTime, new MetricsCounter( checkPointerMonitor::checkPointAccumulatedTotalTimeMillis ) );
    }

    @Override
    public void stop()
    {
        monitors.removeMonitorListener( listener );

        registry.remove( checkPointEvents );
        registry.remove( checkPointTotalTime );
    }

    private static class ReportingCheckPointerMonitor extends CheckPointerMonitorAdapter
    {
        private final EventReporter reporter;
        private final JobScheduler jobScheduler;
        private final String metricName;

        ReportingCheckPointerMonitor( EventReporter reporter, JobScheduler jobScheduler, String metricName )
        {
            this.reporter = reporter;
            this.jobScheduler = jobScheduler;
            this.metricName = metricName;
        }

        @Override
        public void checkPointCompleted( long durationMillis )
        {
            // notify async
            jobScheduler.schedule( Group.METRICS_EVENT, () ->
            {
                TreeMap<String,Gauge> gauges = new TreeMap<>();
                gauges.put( metricName, () -> durationMillis );
                reporter.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
            } );
        }
    }
}
