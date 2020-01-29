/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.function.Supplier;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

@Documented( ".Database data count metrics" )
public class DatabaseCountMetrics extends LifecycleAdapter
{
    private static final String COUNTS_PREFIX = "neo4j.count";

    @Documented( "The total number of relationships in the database" )
    public static final String COUNTS_RELATIONSHIP_TEMPLATE = name( COUNTS_PREFIX, "relationship" );
    @Documented( "The total number of nodes in the database" )
    public static final String COUNTS_NODE_TEMPLATE = name( COUNTS_PREFIX, "node" );

    private final MetricRegistry registry;
    private final Supplier<StoreEntityCounters> countsSource;
    private final String relationshipCounts;
    private final String nodeCounts;

    public DatabaseCountMetrics( String metricsPrefix, MetricRegistry registry, Supplier<StoreEntityCounters> countsSource )
    {
        this.nodeCounts = name( metricsPrefix, COUNTS_NODE_TEMPLATE );
        this.relationshipCounts = name( metricsPrefix, COUNTS_RELATIONSHIP_TEMPLATE );
        this.registry = registry;
        this.countsSource = countsSource;
    }

    @Override
    public void start()
    {
        registry.register( nodeCounts, (Gauge<Long>) () -> countsSource.get().allNodesCountStore( TRACER_SUPPLIER.get() ) );
        registry.register( relationshipCounts, (Gauge<Long>) () -> countsSource.get().allRelationshipsCountStore( TRACER_SUPPLIER.get() ) );
    }

    @Override
    public void stop()
    {
        registry.remove( relationshipCounts );
        registry.remove( nodeCounts );
    }
}
