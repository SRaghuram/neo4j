/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.database;

import com.codahale.metrics.MetricRegistry;

import java.util.Optional;

import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.metrics.database.DatabaseMetricsExtensionFactory.Dependencies;
import org.neo4j.metrics.global.MetricsManager;
import org.neo4j.metrics.output.EventReporter;

public class DatabaseMetricsExtension implements Lifecycle
{
    private final LifeSupport life = new LifeSupport();
    private final ExtensionContext context;
    private final DatabaseMetricsExtensionFactory.Dependencies dependencies;

    DatabaseMetricsExtension( ExtensionContext context, Dependencies dependencies )
    {
        this.context = context;
        this.dependencies = dependencies;
    }

    @Override
    public void init()
    {
        Optional<MetricsManager> optionalManager = getMetricsManager();
        optionalManager.ifPresent( metricsManager ->
        {
            if ( metricsManager.isConfigured() )
            {
                MetricRegistry metricRegistry = metricsManager.getRegistry();
                EventReporter eventReporter = metricsManager.getReporter();
                new DatabaseMetricsExporter( metricRegistry, eventReporter, dependencies.configuration(), context, dependencies, life ).export();
            }
        } );
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

    private Optional<MetricsManager> getMetricsManager()
    {
        try
        {
            return Optional.of( dependencies.metricsManager() );
        }
        catch ( UnsatisfiedDependencyException ude )
        {
            return Optional.empty();
        }
    }
}
