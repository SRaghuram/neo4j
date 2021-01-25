/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.metric;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import java.util.List;
import java.util.SortedSet;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.GlobbingPattern;

public class MetricsRegister
{
    private final MetricRegistry registry;
    private final List<GlobbingPattern> metricsFilter;

    public MetricsRegister( MetricRegistry registry, List<GlobbingPattern> metricsFilter )
    {
        this.registry = registry;
        this.metricsFilter = List.copyOf( metricsFilter );
    }

    public <T extends Metric> void register( String metricName, Supplier<T> metricsCreator )
    {
        if ( metricsFilter.stream().anyMatch( pattern -> pattern.matches( metricName ) ) )
        {
            registry.register( metricName, metricsCreator.get() );
        }
    }

    public void remove( String metricName )
    {
        registry.remove( metricName );
    }

    public void removeMatching( MetricFilter filter )
    {
        registry.removeMatching( filter );
    }

    public SortedSet<String> getNames()
    {
        return registry.getNames();
    }
}
