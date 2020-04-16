/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.MetricRegistry;

import java.util.function.Predicate;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.NamedMemoryPool;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.String.format;

@Documented( ".Global neo4j pools metrics" )
public class GlobalMemoryPoolMetrics extends AbstractMemoryPoolMetrics
{
    private static final String NEO_GLOBAL_POOL_PREFIX = "dbms.pool";
    private static final String NEO_POOL_USAGE_TEMPLATE = name( NEO_GLOBAL_POOL_PREFIX, "%s", "%s", "%s" );

    private final String poolTemplate;

    public GlobalMemoryPoolMetrics( String metricsPrefix, MetricRegistry registry, MemoryPools memoryPools )
    {
        super( name( metricsPrefix, NEO_GLOBAL_POOL_PREFIX ), registry, memoryPools );
        this.poolTemplate = name( metricsPrefix, NEO_POOL_USAGE_TEMPLATE );
    }

    @Override
    protected Predicate<NamedMemoryPool> poolsFilter()
    {
        return pool -> pool.group().isGlobal();
    }

    @Override
    protected String namePoolMetric( NamedMemoryPool pool, String metricName )
    {
        return format( poolTemplate, pool.group().getName(), pool.name(), metricName );
    }
}
