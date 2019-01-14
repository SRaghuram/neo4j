/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.metrics.global.GlobalMetricsExtensionFactory.Dependencies;
import org.neo4j.metrics.output.CompositeEventReporter;
import org.neo4j.metrics.output.EventReporter;
import org.neo4j.metrics.output.EventReporterBuilder;

public class GlobalMetricsExtension implements Lifecycle, MetricsManager
{
    private final LifeSupport life = new LifeSupport();
    private final Log logger;
    private final CompositeEventReporter reporter;
    private final MetricRegistry registry;
    private final ExtensionContext context;
    private final GlobalMetricsExtensionFactory.Dependencies dependencies;
    private boolean configured;

    public GlobalMetricsExtension( ExtensionContext context, Dependencies dependencies )
    {
        LogService logService = dependencies.logService();
        this.context = context;
        this.dependencies = dependencies;
        this.logger = logService.getUserLog( getClass() );
        this.registry = new MetricRegistry();
        this.reporter = new EventReporterBuilder( dependencies.configuration(), registry, logger, context, life, dependencies.fileSystemAbstraction(),
                dependencies.scheduler(), dependencies.portRegister() ).build();
    }

    @Override
    public void init()
    {
        configured = !reporter.isEmpty();
        if ( !configured )
        {
            logger.warn( "Metrics extension reporting is not configured. Please configure one of the available exporting options to be able to use metrics. " +
                    "Metrics extension is disabled." );
            return;
        }
        LogService logService = dependencies.logService();
        Config configuration = dependencies.configuration();
        new GlobalMetricsExporter( registry, configuration, logService, context, dependencies, life ).export();
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

    @Override
    public EventReporter getReporter()
    {
        return reporter;
    }

    @Override
    public MetricRegistry getRegistry()
    {
        return registry;
    }

    @Override
    public boolean isConfigured()
    {
        return configured;
    }
}
