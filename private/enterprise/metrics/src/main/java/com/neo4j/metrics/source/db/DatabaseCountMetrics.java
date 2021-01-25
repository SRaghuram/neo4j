/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;

import java.util.function.Supplier;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database data count metrics" )
public class DatabaseCountMetrics extends LifecycleAdapter
{
    private static final String COUNT_ALL_NODES_TAG = "countAllNodesMetrics";
    private static final String COUNT_ALL_RELATIONSHIP_TAG = "countAllRelationshipMetrics";
    private static final String COUNTS_PREFIX = "neo4j.count";

    @Documented( "The total number of relationships in the database. (gauge)" )
    public static final String COUNTS_RELATIONSHIP_TEMPLATE = name( COUNTS_PREFIX, "relationship" );
    @Documented( "The total number of nodes in the database. (gauge)" )
    public static final String COUNTS_NODE_TEMPLATE = name( COUNTS_PREFIX, "node" );

    private final MetricsRegister registry;
    private final Supplier<StoreEntityCounters> countsSource;
    private final PageCacheTracer pageCacheTracer;
    private final String relationshipCounts;
    private final String nodeCounts;

    public DatabaseCountMetrics( String metricsPrefix, MetricsRegister registry, Supplier<StoreEntityCounters> countsSource, PageCacheTracer pageCacheTracer )
    {
        this.nodeCounts = name( metricsPrefix, COUNTS_NODE_TEMPLATE );
        this.relationshipCounts = name( metricsPrefix, COUNTS_RELATIONSHIP_TEMPLATE );
        this.registry = registry;
        this.countsSource = countsSource;
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    public void start()
    {
        registry.register( nodeCounts, () -> (Gauge<Long>) () ->
        {
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( COUNT_ALL_NODES_TAG ) )
            {
                return countsSource.get().allNodesCountStore( cursorTracer );
            }
        } );
        registry.register( relationshipCounts, () -> (Gauge<Long>) () ->
        {
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( COUNT_ALL_RELATIONSHIP_TAG ) )
            {
                return countsSource.get().allRelationshipsCountStore( cursorTracer );
            }
        } );
    }

    @Override
    public void stop()
    {
        registry.remove( relationshipCounts );
        registry.remove( nodeCounts );
    }
}
