/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

import static com.codahale.metrics.MetricRegistry.name;

public class MemoryBuffersMetrics extends JvmMetrics
{
    private final String memoryBuffer;
    private final MetricRegistry registry;

    public MemoryBuffersMetrics( String metricsPrefix, MetricRegistry registry )
    {
        this.registry = registry;
        this.memoryBuffer = name( metricsPrefix, VM_NAME_PREFIX, "memory.buffer" );
    }

    @Override
    public void start()
    {
        for ( final BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans( BufferPoolMXBean.class ) )
        {
            registry.register(
                    name( memoryBuffer, prettifyName( pool.getName() ), "count" ), (Gauge<Long>) pool::getCount );
            registry.register(
                    name( memoryBuffer, prettifyName( pool.getName() ), "used" ), (Gauge<Long>) pool::getMemoryUsed );
            registry.register(
                    name( memoryBuffer, prettifyName( pool.getName() ), "capacity" ),
                    (Gauge<Long>) pool::getTotalCapacity );
        }
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( memoryBuffer ) );
    }
}
