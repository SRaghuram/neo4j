/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;

import org.neo4j.annotations.documented.Documented;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.String.format;

@Documented( ".JVM memory pools metrics." )
public class JVMMemoryPoolMetrics extends JvmMetrics
{
    private final String memoryPoolPrefix;
    private final String memoryPool;
    private final MetricsRegister registry;

    private static final String MEMORY_POOL_PREFIX = name( VM_NAME_PREFIX, "memory.pool" );
    @Documented( "Estimated number of buffers in the pool. (gauge)" )
    private static final String MEMORY_POOL_USAGE_TEMPLATE = name( MEMORY_POOL_PREFIX, "%s" );

    public JVMMemoryPoolMetrics( String metricsPrefix, MetricsRegister registry )
    {
        this.registry = registry;
        this.memoryPoolPrefix = name( metricsPrefix, MEMORY_POOL_PREFIX );
        this.memoryPool = name( metricsPrefix, MEMORY_POOL_USAGE_TEMPLATE );
    }

    @Override
    public void start()
    {
        for ( final MemoryPoolMXBean memPool : ManagementFactory.getMemoryPoolMXBeans() )
        {
            registry.register( format( memoryPool, prettifyName( memPool.getName() ) ),
                    () -> (Gauge<Long>) () -> memPool.getUsage().getUsed() );
        }
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( memoryPoolPrefix ) );
    }
}
