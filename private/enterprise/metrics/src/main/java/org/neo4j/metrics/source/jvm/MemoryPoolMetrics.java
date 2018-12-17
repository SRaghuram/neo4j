/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;

import static com.codahale.metrics.MetricRegistry.name;

public class MemoryPoolMetrics extends JvmMetrics
{
    private final String memoryPool;
    private final MetricRegistry registry;

    public MemoryPoolMetrics( String metricsPrefix, MetricRegistry registry )
    {
        this.registry = registry;
        this.memoryPool = name( metricsPrefix, VM_NAME_PREFIX, "memory.pool" );
    }

    @Override
    public void start()
    {
        for ( final MemoryPoolMXBean memPool : ManagementFactory.getMemoryPoolMXBeans() )
        {
            registry.register( name( memoryPool, prettifyName( memPool.getName() ) ),
                    (Gauge<Long>) () -> memPool.getUsage().getUsed() );
        }
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( memoryPool ) );
    }
}
