/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Returns the current memory usage of the heap that is used for object allocation.
 * The heap consists of one or more memory pools.
 * The {@code used} and {@code committed} size of the returned memory
 * usage is the sum of those values of all heap memory pools
 * whereas the {@code max} size of the returned memory usage represents
 * the setting of the heap memory which may not be the sum of those of all heap
 * memory pools.
 */
public class HeapMetrics extends JvmMetrics
{
    public static final String HEAP_COMMITTED = name( NAME_PREFIX, "heap.committed" );
    public static final String HEAP_USED = name( NAME_PREFIX, "heap.used" );
    public static final String HEAP_MAX = name( NAME_PREFIX, "heap.max" );

    private final MetricRegistry registry;

    public HeapMetrics( MetricRegistry registry )
    {
        this.registry = registry;
    }

    @Override
    public void start()
    {
        MemoryUsageSupplier memoryUsageSupplier = new MemoryUsageSupplier();
        registry.register( HEAP_COMMITTED, (Gauge<Long>) memoryUsageSupplier::getCommitted );
        registry.register( HEAP_USED, (Gauge<Long>) memoryUsageSupplier::getUsed );
        registry.register( HEAP_MAX, (Gauge<Long>) memoryUsageSupplier::getMax );
    }

    @Override
    public void stop()
    {
        registry.remove( HEAP_MAX );
        registry.remove( HEAP_USED );
        registry.remove( HEAP_COMMITTED );
    }

    private static class MemoryUsageSupplier
    {
        private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        private volatile MemoryUsage lastMemoryUsage = new MemoryUsage( 0, 0, 0, 0 );

        private long getCommitted()
        {
            lastMemoryUsage = memoryMXBean.getHeapMemoryUsage();
            return lastMemoryUsage.getCommitted();
        }

        private long getUsed()
        {
            return lastMemoryUsage.getUsed();
        }

        private long getMax()
        {
            return lastMemoryUsage.getMax();
        }
    }
}
