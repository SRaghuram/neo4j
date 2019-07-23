/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database data count metrics" )
public class DatabaseCountMetrics extends LifecycleAdapter
{
    private static final String COUNTS_PREFIX = "neo4j.count";

    @Documented( "The total number of relationships in the database" )
    public static final String COUNTS_RELATIONSHIP = name( COUNTS_PREFIX, "relationship" );
    @Documented( "The total number of nodes in the database" )
    public static final String COUNTS_NODE = name( COUNTS_PREFIX, "node" );

    private final MetricRegistry registry;
    private final StoreEntityCounters countsSource;

    public DatabaseCountMetrics( MetricRegistry registry, StoreEntityCounters countsSource )
    {
        this.registry = registry;
        this.countsSource = countsSource;
    }

    @Override
    public void start()
    {
        registry.register( COUNTS_NODE, (Gauge<Long>) countsSource::allNodesCountStore );
        registry.register( COUNTS_RELATIONSHIP, (Gauge<Long>) countsSource::allRelationshipsCountStore );
    }

    @Override
    public void stop()
    {
        registry.remove( COUNTS_NODE );
        registry.remove( COUNTS_RELATIONSHIP );
    }
}
