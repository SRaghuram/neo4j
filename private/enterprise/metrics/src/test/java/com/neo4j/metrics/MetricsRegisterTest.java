/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.Supplier;

import org.neo4j.configuration.helpers.GlobbingPattern;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MetricsRegisterTest
{
    @Test
    void filterShouldLetThroughMatchingMetrics()
    {
        MetricRegistry registry = Mockito.mock( MetricRegistry.class );
        MetricsRegister register =
                new MetricsRegister( registry, GlobbingPattern.create( "*metric.1", "prefix.metric.2", "metric*3", "metric.4*", "metri?.5" ) );

        Supplier<MetricsCounter> metricsSupplier = () -> new MetricsCounter( () -> 1L );
        register.register( "metric.1", metricsSupplier );
        register.register( "123.metric.1", metricsSupplier );
        register.register( "metric.2", metricsSupplier );
        register.register( "prefix.metric.2", metricsSupplier );
        register.register( "prefix.metric.22", metricsSupplier );
        register.register( "metric.3", metricsSupplier );
        register.register( "metric3", metricsSupplier );
        register.register( "metricaaa3", metricsSupplier );
        register.register( "metric.4", metricsSupplier );
        register.register( "metric.44", metricsSupplier );
        register.register( "metric.5", metricsSupplier );
        register.register( "metri.5", metricsSupplier );

        verify( registry ).register( eq( "metric.1" ), any() );
        verify( registry ).register( eq( "123.metric.1" ), any() );
        verify( registry ).register( eq( "prefix.metric.2" ), any() );
        verify( registry ).register( eq( "metric.3" ), any() );
        verify( registry ).register( eq( "metric3" ), any() );
        verify( registry ).register( eq( "metricaaa3" ), any() );
        verify( registry ).register( eq( "metric.4" ), any() );
        verify( registry ).register( eq( "metric.44" ), any() );
        verify( registry ).register( eq( "metric.5" ), any() );
        verifyNoMoreInteractions( registry );
    }
}
