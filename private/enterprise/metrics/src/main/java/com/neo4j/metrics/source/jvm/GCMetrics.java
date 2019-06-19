/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.metrics.metric.MetricsCounter;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import org.neo4j.annotations.documented.Documented;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.String.format;

@Documented( ".GC metrics." )
public class GCMetrics extends JvmMetrics
{
    private static final String GC_PREFIX = name( VM_NAME_PREFIX, "gc" );

    @Documented( "Accumulated garbage collection time in milliseconds." )
    private static final String GC_TIME_TEMPLATE = name( GC_PREFIX, "time", "%s" );
    @Documented( "Total number of garbage collections." )
    private static final String GC_COUNT_TEMPLATE = name( GC_PREFIX, "count", "%s" );

    private final String gcPrefix;
    private final String gcTime;
    private final String gcCount;

    private final MetricRegistry registry;

    public GCMetrics( String metricsPrefix, MetricRegistry registry )
    {
        this.registry = registry;
        this.gcPrefix = name( metricsPrefix, GC_PREFIX );
        this.gcTime = name( metricsPrefix, GC_TIME_TEMPLATE );
        this.gcCount = name( metricsPrefix, GC_COUNT_TEMPLATE );
    }

    @Override
    public void start()
    {
        for ( final GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans() )
        {
            registry.register( format( gcTime, prettifyName( gcBean.getName() ) ), new MetricsCounter( gcBean::getCollectionTime ) );
            registry.register( format( gcCount, prettifyName( gcBean.getName() ) ), new MetricsCounter( gcBean::getCollectionCount ) );
        }
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( gcPrefix ) );
    }
}
