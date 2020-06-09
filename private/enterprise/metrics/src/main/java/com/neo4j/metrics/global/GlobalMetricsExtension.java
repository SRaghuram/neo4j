/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory.Dependencies;
import com.neo4j.metrics.output.EventReporterBuilder;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import static com.neo4j.configuration.MetricsSettings.metrics_enabled;

public class GlobalMetricsExtension implements Lifecycle, MetricsManager
{
    private final LifeSupport life = new LifeSupport();
    private final Log logger;
    private final MetricRegistry registry;
    private final ExtensionContext context;
    private final GlobalMetricsExtensionFactory.Dependencies dependencies;
    private final boolean configured;

    public GlobalMetricsExtension( ExtensionContext context, Dependencies dependencies )
    {
        LogService logService = dependencies.logService();
        this.context = context;
        this.dependencies = dependencies;
        this.logger = logService.getUserLog( getClass() );
        this.registry = new MetricRegistry();
        this.configured = new EventReporterBuilder( dependencies.configuration(), registry, logger, context, life, dependencies.fileSystemAbstraction(),
                dependencies.scheduler(), dependencies.portRegister() ).configure();
    }

    @Override
    public void init()
    {
        Config config = dependencies.configuration();

        if ( !config.get( metrics_enabled ) )
        {
            return;
        }

        if ( !configured )
        {
            logger.warn( "Metrics extension reporting is not configured. Please configure one of the available exporting options to be able to use metrics. " +
                    "Metrics extension is disabled." );
            return;
        }
        new GlobalMetricsExporter( registry, config, context, dependencies, life ).export();
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
        // We do not stop life here (it will be stopped on shutdown instead) because this is stopped before individual database life (per database metrics)
        // Those metrics rely on MetricRegistry and EventReporterBuilder to be working
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
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
