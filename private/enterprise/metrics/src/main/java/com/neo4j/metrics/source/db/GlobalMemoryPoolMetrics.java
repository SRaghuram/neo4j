/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryPools;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.String.format;

@Documented( ".Global neo4j pools metrics" )
public class GlobalMemoryPoolMetrics extends LifecycleAdapter
{
    private static final String NEO_GLOBAL_POOL_PREFIX = "dbms.pool";
    private static final String NEO_POOL_USAGE_TEMPLATE = name( NEO_GLOBAL_POOL_PREFIX, "%s", "%s", "%s" );

    private final String metricsPoolPrefix;
    private final String poolTemplate;
    private final MetricRegistry registry;
    private final MemoryPools memoryPools;

    public GlobalMemoryPoolMetrics( String metricsPrefix, MetricRegistry registry, MemoryPools memoryPools )
    {
        this.registry = registry;
        this.memoryPools = memoryPools;
        this.metricsPoolPrefix = name( metricsPrefix, NEO_GLOBAL_POOL_PREFIX );
        this.poolTemplate = name( metricsPrefix, NEO_POOL_USAGE_TEMPLATE );
    }

    @Override
    public void start()
    {
        memoryPools.getPools().stream()
                .filter( pool -> pool.group().isGlobal() )
                .forEach( pool ->
                {
                    registry.register( format( poolTemplate, pool.group().getName(), pool.name(), "usedHeap" ), (Gauge<Long>) pool::usedHeap );
                    registry.register( format( poolTemplate, pool.group().getName(), pool.name(), "usedNative" ), (Gauge<Long>) pool::usedNative );
                    registry.register( format( poolTemplate, pool.group().getName(), pool.name(), "totalUsed" ), (Gauge<Long>) pool::totalUsed );
                    registry.register( format( poolTemplate, pool.group().getName(), pool.name(), "totalSize" ), (Gauge<Long>) pool::totalSize );
                    registry.register( format( poolTemplate, pool.group().getName(), pool.name(), "free" ), (Gauge<Long>) pool::free );
                } );
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( metricsPoolPrefix ) );
    }
}
