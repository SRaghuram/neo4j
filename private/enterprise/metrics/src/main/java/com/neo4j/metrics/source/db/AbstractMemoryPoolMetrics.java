/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;

import java.util.List;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.ScopedMemoryPool;

public abstract class AbstractMemoryPoolMetrics extends LifecycleAdapter
{
    @Documented( "Used or reserved heap memory in bytes. (gauge)" )
    private static final String USED_HEAP = "used_heap";
    @Documented( "Used or reserved native memory in bytes. (gauge)" )
    private static final String USED_NATIVE = "used_native";
    @Documented( "Sum total used heap and native memory in bytes. (gauge)" )
    private static final String TOTAL_USED = "total_used";
    @Documented( "Sum total size of capacity of the heap and/or native memory pool. (gauge)" )
    private static final String TOTAL_SIZE = "total_size";
    @Documented( "Available unused memory in the pool, in bytes. (gauge)" )
    private static final String FREE = "free";
    private final MetricsRegister registry;
    private final String metricsPoolPrefix;
    protected final MemoryPools memoryPools;

    public AbstractMemoryPoolMetrics( String metricsPoolPrefix, MetricsRegister registry, MemoryPools memoryPools )
    {
        this.registry = registry;
        this.memoryPools = memoryPools;
        this.metricsPoolPrefix = metricsPoolPrefix;
    }

    protected abstract String namePoolMetric( ScopedMemoryPool pool, String metricName );

    protected abstract List<? extends ScopedMemoryPool> pools();

    @Override
    public void start()
    {
        pools().forEach( pool ->
        {
            registry.register( prettifyName( namePoolMetric( pool, USED_HEAP ) ), () -> (Gauge<Long>) pool::usedHeap );
            registry.register( prettifyName( namePoolMetric( pool, USED_NATIVE ) ), () -> (Gauge<Long>) pool::usedNative );
            registry.register( prettifyName( namePoolMetric( pool, TOTAL_USED ) ), () -> (Gauge<Long>) pool::totalUsed );
            registry.register( prettifyName( namePoolMetric( pool, TOTAL_SIZE ) ), () -> (Gauge<Long>) pool::totalSize );
            registry.register( prettifyName( namePoolMetric( pool, FREE ) ), () -> (Gauge<Long>) pool::free );
        } );
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( metricsPoolPrefix ) );
    }

    static String prettifyName( String name )
    {
        return name.replace( ' ', '_' );
    }
}
