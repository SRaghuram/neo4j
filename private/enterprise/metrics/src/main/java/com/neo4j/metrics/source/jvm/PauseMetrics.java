/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.VmPauseMonitor;

import static com.codahale.metrics.MetricRegistry.name;

@ServiceProvider
@Documented( ".JVM pause time metrics." )
public class PauseMetrics extends JvmMetrics
{
    // TODO accumulated pause time should be a counter, instead of a gauge. Change on next compatibility break.
    @Documented( "Accumulated detected VM pause time. (gauge)" )
    private static final String PAUSE_TIME = name( VM_NAME_PREFIX, "pause_time" );

    private final String pauseTime;
    private final Monitors monitors;
    private final MetricsRegister registry;
    private final MetricPauseMonitor metricPauseMonitor;

    /**
     * Only for generating documentation. The metrics documentation is generated through
     * service loading which requires a zero-argument constructor.
     */
    public PauseMetrics()
    {
        this( "", null, null );
    }

    public PauseMetrics( String metricsPrefix, MetricsRegister registry, Monitors monitors )
    {
        this.registry = registry;
        this.pauseTime = name( metricsPrefix, PAUSE_TIME );
        this.monitors = monitors;
        metricPauseMonitor = new MetricPauseMonitor();
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( metricPauseMonitor );
        registry.register( pauseTime, () -> (Gauge<Long>) metricPauseMonitor::getPauseTime );
    }

    @Override
    public void stop()
    {
        registry.remove( pauseTime );
        monitors.removeMonitorListener( metricPauseMonitor );
    }

    private static class MetricPauseMonitor extends VmPauseMonitor.Monitor.Adapter
    {
        private final AtomicLong pauseTime = new AtomicLong();

        @Override
        public void pauseDetected( VmPauseMonitor.VmPauseInfo info )
        {
            pauseTime.addAndGet( info.getPauseTime() );
        }

        public long getPauseTime()
        {
            return pauseTime.get();
        }
    }
}
