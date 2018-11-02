/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static com.codahale.metrics.MetricRegistry.name;

public class ThreadMetrics extends JvmMetrics
{
    public static final String THREAD_COUNT = name( JvmMetrics.NAME_PREFIX, "thread.count" );
    public static final String THREAD_TOTAL = name( JvmMetrics.NAME_PREFIX, "thread.total" );

    private final MetricRegistry registry;
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public ThreadMetrics( MetricRegistry registry )
    {
        this.registry = registry;
    }

    @Override
    public void start()
    {
        registry.register( THREAD_COUNT, (Gauge<Integer>) Thread::activeCount );
        registry.register( THREAD_TOTAL, (Gauge<Integer>) threadMXBean::getThreadCount );
    }

    @Override
    public void stop()
    {
        registry.remove( THREAD_COUNT );
        registry.remove( THREAD_TOTAL );
    }
}

