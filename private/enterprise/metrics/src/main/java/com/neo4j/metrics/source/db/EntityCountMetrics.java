/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;
import com.neo4j.metrics.source.MetricGroup;
import com.neo4j.metrics.source.Metrics;

import java.util.function.Supplier;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;

import static com.codahale.metrics.MetricRegistry.name;

@ServiceProvider
@Documented( ".Database data metrics" )
public class EntityCountMetrics extends Metrics
{
    private static final String COUNTS_PREFIX = "ids_in_use";

    @Documented( "The total number of different relationship types stored in the database. (gauge)" )
    private static final String COUNTS_RELATIONSHIP_TYPE_TEMPLATE = name( COUNTS_PREFIX, "relationship_type" );
    @Documented( "The total number of different property names used in the database. (gauge)" )
    private static final String COUNTS_PROPERTY_TEMPLATE = name( COUNTS_PREFIX, "property" );
    @Documented( "The total number of relationships stored in the database. (gauge)" )
    private static final String COUNTS_RELATIONSHIP_TEMPLATE = name( COUNTS_PREFIX, "relationship" );
    @Documented( "The total number of nodes stored in the database. (gauge)" )
    private static final String COUNTS_NODE_TEMPLATE = name( COUNTS_PREFIX, "node" );

    private final String countsRelationshipType;
    private final String countsProperty;
    private final String countsRelationship;
    private final String countsNode;

    private final MetricsRegister registry;
    private final Supplier<StoreEntityCounters> storeEntityCounters;

    /**
     * Only for generating documentation. The metrics documentation is generated through
     * service loading which requires a zero-argument constructor.
     */
    public EntityCountMetrics()
    {
        this( "", null, null );
    }

    public EntityCountMetrics( String metricsPrefix, MetricsRegister registry, Supplier<StoreEntityCounters> storeEntityCounters )
    {
        super( MetricGroup.GENERAL );
        this.countsRelationshipType = name( metricsPrefix, COUNTS_RELATIONSHIP_TYPE_TEMPLATE );
        this.countsProperty = name( metricsPrefix, COUNTS_PROPERTY_TEMPLATE );
        this.countsRelationship = name( metricsPrefix, COUNTS_RELATIONSHIP_TEMPLATE );
        this.countsNode = name( metricsPrefix, COUNTS_NODE_TEMPLATE );
        this.registry = registry;
        this.storeEntityCounters = storeEntityCounters;
    }

    @Override
    public void start()
    {
        registry.register( countsNode, () -> (Gauge<Long>) () -> storeEntityCounters.get().nodes() );
        registry.register( countsRelationship, () -> (Gauge<Long>) () -> storeEntityCounters.get().relationships() );
        registry.register( countsProperty, () -> (Gauge<Long>) () -> storeEntityCounters.get().properties() );
        registry.register( countsRelationshipType, () -> (Gauge<Long>) () -> storeEntityCounters.get().relationshipTypes() );
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
