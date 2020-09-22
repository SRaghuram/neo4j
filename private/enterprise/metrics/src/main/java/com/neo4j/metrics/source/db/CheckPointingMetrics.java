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
import org.neo4j.kernel.impl.transaction.stats.CheckpointCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database checkpointing metrics" )
public class CheckPointingMetrics extends LifecycleAdapter
{
    private static final String CHECK_POINT_PREFIX = "check_point";

    @Documented( "The total number of check point events executed so far. (counter)" )
    private static final String CHECK_POINT_EVENTS_TEMPLATE = name( CHECK_POINT_PREFIX, "events" );
    @Documented( "The total time, in milliseconds, spent in check pointing so far. (counter)" )
    private static final String CHECK_POINT_TOTAL_TIME_TEMPLATE = name( CHECK_POINT_PREFIX, "total_time" );
    @Documented( "The duration, in milliseconds, of the last check point event. (gauge)" )
    private static final String CHECK_POINT_DURATION_TEMPLATE = name( CHECK_POINT_PREFIX, "duration" );

    private final String checkPointEvents;
    private final String checkPointTotalTime;
    private final String checkPointDuration;

    private final MetricsRegister registry;
    private final CheckpointCounters checkpointCounters;

    public CheckPointingMetrics( String metricsPrefix, MetricsRegister registry, CheckpointCounters checkpointCounters )
    {
        this.checkPointEvents = name( metricsPrefix, CHECK_POINT_EVENTS_TEMPLATE );
        this.checkPointTotalTime = name( metricsPrefix, CHECK_POINT_TOTAL_TIME_TEMPLATE );
        this.checkPointDuration = name( metricsPrefix, CHECK_POINT_DURATION_TEMPLATE );
        this.registry = registry;
        this.checkpointCounters = checkpointCounters;
    }

    @Override
    public void start()
    {
        registry.register( checkPointEvents, () -> new MetricsCounter( checkpointCounters::numberOfCheckPoints ) );
        registry.register( checkPointTotalTime, () -> new MetricsCounter( checkpointCounters::checkPointAccumulatedTotalTimeMillis ) );
        registry.register( checkPointDuration, () -> (Gauge<Long>) checkpointCounters::lastCheckpointTimeMillis );
    }

    @Override
    public void stop()
    {
        registry.remove( checkPointEvents );
        registry.remove( checkPointTotalTime );
        registry.remove( checkPointDuration );
    }
}
