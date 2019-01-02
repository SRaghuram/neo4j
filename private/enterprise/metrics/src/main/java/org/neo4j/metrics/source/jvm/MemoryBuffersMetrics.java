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

import org.neo4j.kernel.impl.annotations.Documented;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.String.format;

@Documented( ".JVM memory buffers metrics." )
public class MemoryBuffersMetrics extends JvmMetrics
{
    private static final String MEMORY_BUFFER_PREFIX = name( VM_NAME_PREFIX, "memory.buffer" );

    @Documented( "Estimated number of buffers in the pool." )
    private static final String MEMORY_BUFFER_COUNT_TEMPLATE = name( MEMORY_BUFFER_PREFIX, "%s", "count" );
    @Documented( "Estimated amount of memory used by the pool." )
    private static final String MEMORY_BUFFER_USED_TEMPLATE = name( MEMORY_BUFFER_PREFIX, "%s", "used" );
    @Documented( "Estimated total capacity of buffers in the pool." )
    private static final String MEMORY_BUFFER_CAPACITY_TEMPLATE = name( MEMORY_BUFFER_PREFIX, "%s", "capacity" );

    private final MetricRegistry registry;
    private final String memoryBufferPrefix;
    private final String memoryBufferCount;
    private final String memoryBufferUsed;
    private final String memoryBufferCapacity;

    public MemoryBuffersMetrics( String metricsPrefix, MetricRegistry registry )
    {
        this.registry = registry;
        this.memoryBufferPrefix = name( metricsPrefix, MEMORY_BUFFER_PREFIX );
        this.memoryBufferCount = name( metricsPrefix, MEMORY_BUFFER_COUNT_TEMPLATE );
        this.memoryBufferUsed = name( metricsPrefix, MEMORY_BUFFER_USED_TEMPLATE );
        this.memoryBufferCapacity = name( metricsPrefix, MEMORY_BUFFER_CAPACITY_TEMPLATE );
    }

    @Override
    public void start()
    {
        for ( final BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans( BufferPoolMXBean.class ) )
        {
            String poolPrettyName = prettifyName( pool.getName() );
            registry.register( format( memoryBufferCount, poolPrettyName ), (Gauge<Long>) pool::getCount );
            registry.register( format( memoryBufferUsed, poolPrettyName ), (Gauge<Long>) pool::getMemoryUsed );
            registry.register( format( memoryBufferCapacity, poolPrettyName ), (Gauge<Long>) pool::getTotalCapacity );
        }
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( memoryBufferPrefix ) );
    }
}
