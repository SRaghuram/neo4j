/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import org.neo4j.annotations.documented.Documented;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".JVM threads metrics." )
public class ThreadMetrics extends JvmMetrics
{
    @Documented( "Estimated number of active threads in the current thread group. (gauge)" )
    private static final String THREAD_COUNT_TEMPLATE = name( VM_NAME_PREFIX, "thread.count" );
    @Documented( "The total number of live threads including daemon and non-daemon threads. (gauge)" )
    private static final String THREAD_TOTAL_TEMPLATE = name( VM_NAME_PREFIX, "thread.total" );

    private final String threadCount;
    private final String threadTotal;

    private final MetricsRegister registry;
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public ThreadMetrics( String metricsPrefix, MetricsRegister registry )
    {
        this.registry = registry;
        this.threadCount = name( metricsPrefix, THREAD_COUNT_TEMPLATE );
        this.threadTotal = name( metricsPrefix, THREAD_TOTAL_TEMPLATE );
    }

    @Override
    public void start()
    {
        registry.register( threadCount, () -> (Gauge<Integer>) Thread::activeCount );
        registry.register( threadTotal, () -> (Gauge<Integer>) threadMXBean::getThreadCount );
    }

    @Override
    public void stop()
    {
        registry.remove( threadCount );
        registry.remove( threadTotal );
    }
}

