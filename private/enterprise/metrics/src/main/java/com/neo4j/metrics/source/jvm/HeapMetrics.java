/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.annotations.service.ServiceProvider;

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
@ServiceProvider
@Documented( ".JVM Heap metrics." )
public class HeapMetrics extends JvmMetrics
{
    @Documented( "Amount of memory (in bytes) guaranteed to be available for use by the JVM. (gauge)" )
    public static final String HEAP_COMMITTED_TEMPLATE = name( VM_NAME_PREFIX, "heap.committed" );
    @Documented( "Amount of memory (in bytes) currently used. (gauge)" )
    public static final String HEAP_USED_TEMPLATE = name( VM_NAME_PREFIX, "heap.used" );
    @Documented( "Maximum amount of heap memory (in bytes) that can be used. (gauge)" )
    public static final String HEAP_MAX_TEMPLATE = name( VM_NAME_PREFIX, "heap.max" );

    private final MetricsRegister registry;
    private final String heapCommitted;
    private final String heapUsed;
    private final String heapMax;

    /**
     * Only for generating documentation. The metrics documentation is generated through
     * service loading which requires a zero-argument constructor.
     */
    public HeapMetrics()
    {
        this( "", null );
    }

    public HeapMetrics( String metricsPrefix, MetricsRegister registry )
    {
        this.registry = registry;
        this.heapCommitted = name( metricsPrefix, HEAP_COMMITTED_TEMPLATE );
        this.heapUsed = name( metricsPrefix, HEAP_USED_TEMPLATE );
        this.heapMax = name( metricsPrefix, HEAP_MAX_TEMPLATE );
    }

    @Override
    public void start()
    {
        MemoryUsageSupplier memoryUsageSupplier = new MemoryUsageSupplier();
        registry.register( heapCommitted, () -> (Gauge<Long>) memoryUsageSupplier::getCommitted );
        registry.register( heapUsed, () -> (Gauge<Long>) memoryUsageSupplier::getUsed );
        registry.register( heapMax, () -> (Gauge<Long>) memoryUsageSupplier::getMax );
    }

    @Override
    public void stop()
    {
        registry.remove( heapMax );
        registry.remove( heapUsed );
        registry.remove( heapCommitted );
    }

    private static class MemoryUsageSupplier
    {
        private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        private volatile MemoryUsage lastMemoryUsage = memoryMXBean.getHeapMemoryUsage();

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
