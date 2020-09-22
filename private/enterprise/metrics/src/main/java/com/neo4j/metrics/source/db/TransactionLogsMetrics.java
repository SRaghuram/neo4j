/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.impl.transaction.stats.TransactionLogCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database transaction log metrics" )
public class TransactionLogsMetrics extends LifecycleAdapter
{
    private static final String TX_LOG_PREFIX = "log";

    @Documented( "The total number of transaction log rotations executed so far. (counter)" )
    private static final String LOG_ROTATION_EVENTS_TEMPLATE = name( TX_LOG_PREFIX, "rotation_events" );
    @Documented( "The total time, in milliseconds, spent in rotating transaction logs so far. (counter)" )
    private static final String LOG_ROTATION_TOTAL_TIME_TEMPLATE = name( TX_LOG_PREFIX, "rotation_total_time" );
    @Documented( "The duration, in milliseconds, of the last log rotation event. (gauge)" )
    private static final String LOG_ROTATION_DURATION_TEMPLATE = name( TX_LOG_PREFIX, "rotation_duration" );
    @Documented( "The total number of bytes appended to transaction log. (counter)" )
    private static final String LOG_APPENDED_BYTES = name( TX_LOG_PREFIX, "appended_bytes" );

    private final String logRotationEvents;
    private final String logRotationTotalTime;
    private final String logRotationDuration;
    private final String logAppendedBytes;

    private final MetricsRegister registry;
    private final TransactionLogCounters logCounters;

    public TransactionLogsMetrics( String metricsPrefix, MetricsRegister registry, TransactionLogCounters logCounters )
    {
        this.logRotationEvents = name( metricsPrefix, LOG_ROTATION_EVENTS_TEMPLATE );
        this.logRotationTotalTime = name( metricsPrefix, LOG_ROTATION_TOTAL_TIME_TEMPLATE );
        this.logRotationDuration = name( metricsPrefix, LOG_ROTATION_DURATION_TEMPLATE );
        this.logAppendedBytes = name( metricsPrefix, LOG_APPENDED_BYTES );
        this.registry = registry;
        this.logCounters = logCounters;
    }

    @Override
    public void start()
    {
        registry.register( logRotationEvents, () -> new MetricsCounter( logCounters::numberOfLogRotations ) );
        registry.register( logRotationTotalTime, () -> new MetricsCounter( logCounters::logRotationAccumulatedTotalTimeMillis ) );
        registry.register( logAppendedBytes, () -> new MetricsCounter( logCounters::appendedBytes ) );
        registry.register( logRotationDuration, () -> (Gauge<Long>) logCounters::lastLogRotationTimeMillis  );
    }

    @Override
    public void stop()
    {
        registry.remove( logRotationDuration );
        registry.remove( logRotationEvents );
        registry.remove( logRotationTotalTime );
        registry.remove( logAppendedBytes );
    }
}
