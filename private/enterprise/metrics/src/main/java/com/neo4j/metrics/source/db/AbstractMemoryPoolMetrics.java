/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.NamedMemoryPool;

public abstract class AbstractMemoryPoolMetrics extends LifecycleAdapter
{
    private static final String USED_HEAP = "used_heap";
    private static final String USED_NATIVE = "used_native";
    private static final String TOTAL_USED = "total_used";
    private static final String TOTAL_SIZE = "total_size";
    private static final String FREE = "free";
    private final MetricRegistry registry;
    private final String metricsPoolPrefix;
    private final MemoryPools memoryPools;

    public AbstractMemoryPoolMetrics( String metricsPoolPrefix, MetricRegistry registry, MemoryPools memoryPools )
    {
        this.registry = registry;
        this.memoryPools = memoryPools;
        this.metricsPoolPrefix = metricsPoolPrefix;
    }

    protected abstract String namePoolMetric( NamedMemoryPool pool, String metricName );

    protected abstract Predicate<NamedMemoryPool> poolsFilter();

    @Override
    public void start()
    {
        memoryPools.getPools().stream()
                .flatMap( p -> p.getSubPools().isEmpty() ? Stream.of( p ) : p.getSubPools().stream() )
                .filter( poolsFilter() )
                .forEach( pool ->
                {
                    registry.register( prettifyName( namePoolMetric( pool, USED_HEAP ) ), (Gauge<Long>) pool::usedHeap );
                    registry.register( prettifyName( namePoolMetric( pool, USED_NATIVE ) ), (Gauge<Long>) pool::usedNative );
                    registry.register( prettifyName( namePoolMetric( pool, TOTAL_USED ) ), (Gauge<Long>) pool::totalUsed );
                    registry.register( prettifyName( namePoolMetric( pool, TOTAL_SIZE ) ), (Gauge<Long>) pool::totalSize );
                    registry.register( prettifyName( namePoolMetric( pool, FREE ) ), (Gauge<Long>) pool::free );
                } );
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( metricsPoolPrefix ) );
    }

    static String prettifyName( String name )
    {
        return name.toLowerCase().replace( ' ', '_' );
    }
}
