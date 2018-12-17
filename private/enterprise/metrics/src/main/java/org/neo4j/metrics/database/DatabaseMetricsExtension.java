/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.database;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.metrics.database.DatabaseMetricsKernelExtensionFactory.Dependencies;
import org.neo4j.metrics.global.MetricsProvider;
import org.neo4j.metrics.output.EventReporter;

public class DatabaseMetricsExtension implements Lifecycle
{
    private final LifeSupport life = new LifeSupport();

    DatabaseMetricsExtension( KernelContext context, Dependencies dependencies )
    {
        Config configuration = dependencies.configuration();
        MetricsProvider metricsProvider = dependencies.metricsProvider();
        MetricRegistry metricRegistry = metricsProvider.getRegistry();
        EventReporter eventReporter = metricsProvider.getReporter();
        new DatabaseMetricsBuilder( metricRegistry, eventReporter, configuration, context, dependencies, life ).build();
    }

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start()
    {
        life.start();
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
    }
}
