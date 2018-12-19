/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database data metrics" )
public class EntityCountMetrics extends LifecycleAdapter
{
    private static final String COUNTS_PREFIX = "ids_in_use";

    @Documented( "The total number of different relationship types stored in the database" )
    private final String countsRelationshipType;
    @Documented( "The total number of different property names used in the database" )
    private final String countsProperty;
    @Documented( "The total number of relationships stored in the database" )
    private final String countsRelationship;
    @Documented( "The total number of nodes stored in the database" )
    private final String countsNode;

    private final MetricRegistry registry;
    private final StoreEntityCounters storeEntityCounters;

    public EntityCountMetrics( String metricsPrefix, MetricRegistry registry, StoreEntityCounters storeEntityCounters )
    {
        this.countsRelationshipType = name( metricsPrefix, COUNTS_PREFIX, "relationship_type" );
        this.countsProperty = name( metricsPrefix, COUNTS_PREFIX, "property" );
        this.countsRelationship = name( metricsPrefix, COUNTS_PREFIX, "relationship" );
        this.countsNode = name( metricsPrefix, COUNTS_PREFIX, "node" );
        this.registry = registry;
        this.storeEntityCounters = storeEntityCounters;
    }

    @Override
    public void start()
    {
        registry.register( countsNode, (Gauge<Long>) storeEntityCounters::nodes );
        registry.register( countsRelationship, (Gauge<Long>) storeEntityCounters::relationships );
        registry.register( countsProperty, (Gauge<Long>) storeEntityCounters::properties );
        registry.register( countsRelationshipType, (Gauge<Long>) storeEntityCounters::relationshipTypes );
    }

    @Override
    public void stop()
    {
        registry.remove( countsNode );
        registry.remove( countsRelationship );
        registry.remove( countsProperty );
        registry.remove( countsRelationshipType );
    }
}
