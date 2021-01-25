/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.neo4j.metrics.metric.MetricsRegister;

import java.util.List;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.ScopedMemoryPool;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.String.format;

@Documented( ".Global neo4j pools metrics" )
public class GlobalMemoryPoolMetrics extends AbstractMemoryPoolMetrics
{
    private static final String NEO_GLOBAL_POOL_PREFIX = "dbms.pool";
    private static final String NEO_POOL_USAGE_TEMPLATE = name( NEO_GLOBAL_POOL_PREFIX, "%s", "%s" );

    private final String poolTemplate;

    public GlobalMemoryPoolMetrics( String metricsPrefix, MetricsRegister registry, MemoryPools memoryPools )
    {
        super( name( metricsPrefix, NEO_GLOBAL_POOL_PREFIX ), registry, memoryPools );
        this.poolTemplate = name( metricsPrefix, NEO_POOL_USAGE_TEMPLATE );
    }

    @Override
    protected List<GlobalMemoryGroupTracker> pools()
    {
        return memoryPools.getPools();
    }

    @Override
    protected String namePoolMetric( ScopedMemoryPool pool, String metricName )
    {
        return format( poolTemplate, pool.group().getName().toLowerCase(), metricName.toLowerCase() );
    }
}
