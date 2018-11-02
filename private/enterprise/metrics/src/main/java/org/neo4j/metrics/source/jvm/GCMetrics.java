/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import static com.codahale.metrics.MetricRegistry.name;

public class GCMetrics extends JvmMetrics
{
    public static final String GC_PREFIX = name( NAME_PREFIX, "gc" );
    public static final String GC_TIME = name( GC_PREFIX, "time" );
    public static final String GC_COUNT = name( GC_PREFIX, "count" );

    private final MetricRegistry registry;

    public GCMetrics( MetricRegistry registry )
    {
        this.registry = registry;
    }

    @Override
    public void start()
    {
        for ( final GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans() )
        {
            registry.register( name( GC_TIME, prettifyName( gcBean.getName() ) ),
                    (Gauge<Long>) gcBean::getCollectionTime );
            registry.register( name( GC_COUNT, prettifyName( gcBean.getName() ) ),
                    (Gauge<Long>) gcBean::getCollectionCount );
        }
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( GC_PREFIX ) );
    }
}
