/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.jvm;

import com.codahale.metrics.MetricRegistry;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import org.neo4j.metrics.metric.MetricsCounter;

import static com.codahale.metrics.MetricRegistry.name;

public class GCMetrics extends JvmMetrics
{
    private final String gcPrefix;
    private final String gcTime;
    private final String gcCount;

    private final MetricRegistry registry;

    public GCMetrics( String metricsPrefix, MetricRegistry registry )
    {
        this.registry = registry;
        this.gcPrefix = name( metricsPrefix, VM_NAME_PREFIX, "gc" );
        this.gcTime = name( gcPrefix, "time" );
        this.gcCount = name( gcPrefix, "count" );
    }

    @Override
    public void start()
    {
        for ( final GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans() )
        {
            registry.register( name( gcTime, prettifyName( gcBean.getName() ) ), new MetricsCounter( gcBean::getCollectionTime ) );
            registry.register( name( gcCount, prettifyName( gcBean.getName() ) ), new MetricsCounter( gcBean::getCollectionCount ) );
        }
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( gcPrefix ) );
    }
}
