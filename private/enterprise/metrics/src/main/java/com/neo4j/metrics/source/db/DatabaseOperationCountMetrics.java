/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.metrics.metric.MetricsCounter;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database operation count metrics" )
public class DatabaseOperationCountMetrics extends LifecycleAdapter
{
    private static final String SERVER_PREFIX = "db.operation.count";

    @Documented( "Count of successful database create operations." )
    public static final String DATABASE_CREATE_COUNT = name( SERVER_PREFIX, "create" );
    @Documented( "Count of successful database start operations." )
    public static final String DATABASE_START_COUNT = name( SERVER_PREFIX, "start" );
    @Documented( "Count of successful database stop operations." )
    public static final String DATABASE_STOP_COUNT = name( SERVER_PREFIX, "stop" );
    @Documented( "Count of successful database drop operations." )
    public static final String DATABASE_DROP_COUNT = name( SERVER_PREFIX, "drop" );
    @Documented( "Count of failed database operations." )
    public static final String DATABASE_FAILED_COUNT = name( SERVER_PREFIX, "failed" );
    @Documented( "Count of database operations which failed previously but have recovered." )
    public static final String DATABASE_RECOVERED_COUNT = name( SERVER_PREFIX, "recovered" );

    private final String countCreate;
    private final String countStart;
    private final String countStop;
    private final String countDrop;
    private final String countFailed;
    private final String countRecovered;

    private final MetricRegistry registry;
    private final DatabaseOperationCounts counter;

    public DatabaseOperationCountMetrics( String metricsPrefix, MetricRegistry registry, DatabaseOperationCounts counter )
    {
        this.registry = registry;
        this.counter = counter;

        this.countCreate = name( metricsPrefix, DATABASE_CREATE_COUNT );
        this.countStart = name( metricsPrefix, DATABASE_START_COUNT );
        this.countStop = name( metricsPrefix, DATABASE_STOP_COUNT );
        this.countDrop = name( metricsPrefix, DATABASE_DROP_COUNT );
        this.countFailed = name( metricsPrefix, DATABASE_FAILED_COUNT );
        this.countRecovered = name( metricsPrefix, DATABASE_RECOVERED_COUNT );
    }

    @Override
    public void start()
    {
        registry.register( countCreate, new MetricsCounter( counter::createCount ) );
        registry.register( countStart, new MetricsCounter( counter::startCount ) );
        registry.register( countStop, new MetricsCounter( counter::stopCount ) );
        registry.register( countDrop, new MetricsCounter( counter::dropCount ) );
        registry.register( countFailed, new MetricsCounter( counter::failedCount ) );
        registry.register( countRecovered, new MetricsCounter( counter::recoveredCount ) );
    }

    @Override
    public void stop()
    {
        registry.remove( countCreate );
        registry.remove( countStart );
        registry.remove( countStop );
        registry.remove( countDrop );
        registry.remove( countFailed );
        registry.remove( countRecovered );
    }
}
