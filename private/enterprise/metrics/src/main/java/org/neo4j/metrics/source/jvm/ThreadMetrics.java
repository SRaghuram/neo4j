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
    private final String threadCount;
    private final String threadTotal;

    private final MetricRegistry registry;
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public ThreadMetrics( String metricsPrefix, MetricRegistry registry )
    {
        this.registry = registry;
        this.threadCount = name( metricsPrefix, VM_NAME_PREFIX, "thread.count" );
        this.threadTotal = name( metricsPrefix, VM_NAME_PREFIX, "thread.total" );
    }

    @Override
    public void start()
    {
        registry.register( threadCount, (Gauge<Integer>) Thread::activeCount );
        registry.register( threadTotal, (Gauge<Integer>) threadMXBean::getThreadCount );
    }

    @Override
    public void stop()
    {
        registry.remove( threadCount );
        registry.remove( threadTotal );
    }
}

