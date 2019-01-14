/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.function.Supplier;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database data metrics" )
public class EntityCountMetrics extends LifecycleAdapter
{
    private static final String COUNTS_PREFIX = "neo4j.ids_in_use";

    @Documented( "The total number of different relationship types stored in the database" )
    public static final String COUNTS_RELATIONSHIP_TYPE = name( COUNTS_PREFIX, "relationship_type" );
    @Documented( "The total number of different property names used in the database" )
    public static final String COUNTS_PROPERTY = name( COUNTS_PREFIX, "property" );
    @Documented( "The total number of relationships stored in the database" )
    public static final String COUNTS_RELATIONSHIP = name( COUNTS_PREFIX, "relationship" );
    @Documented( "The total number of nodes stored in the database" )
    public static final String COUNTS_NODE = name( COUNTS_PREFIX, "node" );

    private final MetricRegistry registry;
    private final Supplier<StoreEntityCounters> storeEntityCountersSupplier;

    public EntityCountMetrics( MetricRegistry registry, Supplier<StoreEntityCounters> storeEntityCountersSupplier )
    {
        this.registry = registry;
        this.storeEntityCountersSupplier = storeEntityCountersSupplier;
    }

    @Override
    public void start()
    {
        StoreEntityCounters counters = storeEntityCountersSupplier.get();
        registry.register( COUNTS_NODE, (Gauge<Long>) counters::nodes );
        registry.register( COUNTS_RELATIONSHIP, (Gauge<Long>) counters::relationships );
        registry.register( COUNTS_PROPERTY, (Gauge<Long>) counters::properties );
        registry.register( COUNTS_RELATIONSHIP_TYPE, (Gauge<Long>) counters::relationshipTypes );
    }

    @Override
    public void stop()
    {
        registry.remove( COUNTS_NODE );
        registry.remove( COUNTS_RELATIONSHIP );
        registry.remove( COUNTS_PROPERTY );
        registry.remove( COUNTS_RELATIONSHIP_TYPE );
    }
}
