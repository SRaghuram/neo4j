/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.neo4j.metrics.metric.MetricsRegister;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.ScopedMemoryPool;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Documented( ".Database neo4j pools metrics" )
public class DatabaseMemoryPoolMetrics extends AbstractMemoryPoolMetrics
{
    private static final String NEO_DATABASE_POOL_PREFIX = "pool";
    private static final String NEO_DATABASE_POOL_USAGE_TEMPLATE = name( NEO_DATABASE_POOL_PREFIX, "%s", "%s", "%s" );

    private final String poolTemplate;
    private final String databaseName;

    public DatabaseMemoryPoolMetrics( String metricsPrefix, MetricsRegister registry, MemoryPools memoryPools, String databaseName )
    {
        super( name( metricsPrefix, NEO_DATABASE_POOL_PREFIX ), registry, memoryPools );
        this.poolTemplate = name( metricsPrefix, NEO_DATABASE_POOL_USAGE_TEMPLATE );
        this.databaseName = requireNonNull( databaseName );
    }

    @Override
    protected List<ScopedMemoryPool> pools()
    {
        return memoryPools.getPools().stream()
                .flatMap( pool -> pool.getDatabasePools().stream() )
                .filter( pool -> databaseName.equals( pool.databaseName() ) )
                .collect( Collectors.toList() );
    }

    @Override
    protected String namePoolMetric( ScopedMemoryPool pool, String metricName )
    {
        return format( poolTemplate, pool.group().getName().toLowerCase(), pool.databaseName().toLowerCase(), metricName.toLowerCase() );
    }
}
