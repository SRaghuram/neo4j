/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.dbms.database.DatabaseOperationCountMonitor;
import com.neo4j.metrics.metric.MetricsCounter;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Multi database count metrics" )
public class DatabaseOperationCountMetrics extends LifecycleAdapter
{
    private static final String SERVER_PREFIX = "db.operation.count";

    @Documented( "Database create operations." )
    public static final String DATABASE_CREATE_COUNT = name( SERVER_PREFIX, "create" );
    @Documented( "Database start operations." )
    public static final String DATABASE_START_COUNT = name( SERVER_PREFIX, "start" );
    @Documented( "Database stop operations." )
    public static final String DATABASE_STOP_COUNT = name( SERVER_PREFIX, "stop" );
    @Documented( "Database drop operations." )
    public static final String DATABASE_DROP_COUNT = name( SERVER_PREFIX, "drop" );
    @Documented( "Count of failed database operations." )
    public static final String DATABASE_FAILED_COUNT = name( SERVER_PREFIX, "failed" );
    @Documented( "Count of database operations recovered." )
    public static final String DATABASE_RECOVERED_COUNT = name( SERVER_PREFIX, "recovered" );

    private final String countCreate;
    private final String countStart;
    private final String countStop;
    private final String countDrop;
    private final String countFailed;
    private final String countRecovered;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final DatabaseOperationCountMetric databaseCountMetric = new DatabaseOperationCountMetric();

    public DatabaseOperationCountMetrics( String metricsPrefix, MetricRegistry registry, Monitors monitors )
    {
        this.registry = registry;
        this.monitors = monitors;

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
        monitors.addMonitorListener( databaseCountMetric );

        registry.register( countCreate, new MetricsCounter( databaseCountMetric::createCount ) );
        registry.register( countStart, new MetricsCounter( databaseCountMetric::startCount ) );
        registry.register( countStop, new MetricsCounter( databaseCountMetric::stopCount ) );
        registry.register( countDrop, new MetricsCounter( databaseCountMetric::dropCount ) );
        registry.register( countFailed, new MetricsCounter( databaseCountMetric::failedCount ) );
        registry.register( countRecovered, new MetricsCounter( databaseCountMetric::recoveredCount ) );
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

        monitors.removeMonitorListener( databaseCountMetric );
    }

    static class DatabaseOperationCountMetric implements DatabaseOperationCountMonitor
    {
        private AtomicLong createCount = new AtomicLong( 0 );
        private AtomicLong startCount = new AtomicLong( 0 );
        private AtomicLong stopCount = new AtomicLong( 0 );
        private AtomicLong dropCount = new AtomicLong( 0 );
        private AtomicLong failedCount = new AtomicLong( 0 );
        private AtomicLong recoveredCount = new AtomicLong( 0 );

        @Override
        public long startCount()
        {
            return startCount.get();
        }

        @Override
        public long createCount()
        {
            return createCount.get();
        }

        @Override
        public long stopCount()
        {
            return stopCount.get();
        }

        @Override
        public long dropCount()
        {
            return dropCount.get();
        }

        @Override
        public long failedCount()
        {
            return failedCount.get();
        }

        @Override
        public long recoveredCount()
        {
            return recoveredCount.get();
        }

        @Override
        public void increaseCreateCount()
        {
            createCount.incrementAndGet();
        }

        @Override
        public void increaseStartCount()
        {
            startCount.incrementAndGet();
        }

        @Override
        public void increaseStopCount()
        {
            stopCount.incrementAndGet();
        }

        @Override
        public void increaseDropCount()
        {
            dropCount.incrementAndGet();
        }

        @Override
        public void increaseFailedCount()
        {
            failedCount.incrementAndGet();
        }

        @Override
        public void increaseRecoveredCount()
        {
            recoveredCount.incrementAndGet();
        }

        @Override
        public void resetCounts()
        {
            createCount.set( 0 );
            startCount.set( 0 );
            stopCount.set( 0 );
            dropCount.set( 0 );
            failedCount.set( 0 );
            recoveredCount.set( 0 );
        }
    }
}
