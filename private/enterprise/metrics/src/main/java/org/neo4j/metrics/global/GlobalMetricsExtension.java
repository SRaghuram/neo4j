/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.metrics.global.GlobalMetricsKernelExtensionFactory.Dependencies;
import org.neo4j.metrics.output.CompositeEventReporter;
import org.neo4j.metrics.output.EventReporter;
import org.neo4j.metrics.output.EventReporterBuilder;
import org.neo4j.scheduler.JobScheduler;

public class GlobalMetricsExtension implements Lifecycle, MetricsProvider
{
    private final LifeSupport life = new LifeSupport();
    private final Log logger;
    private final CompositeEventReporter reporter;
    private final MetricRegistry registry;

    GlobalMetricsExtension( KernelContext context, Dependencies dependencies )
    {
        LogService logService = dependencies.logService();
        Config configuration = dependencies.configuration();
        FileSystemAbstraction fileSystem = dependencies.fileSystemAbstraction();
        JobScheduler scheduler = dependencies.scheduler();
        logger = logService.getUserLog( getClass() );

        registry = new MetricRegistry();
        reporter = new EventReporterBuilder( configuration, registry, logger, context, life, fileSystem, scheduler, dependencies.portRegister() ).build();
        new GlobalMetricsBuilder( registry, configuration, logService, context, dependencies, life ).build();
    }

    @Override
    public void init()
    {
        if ( reporter.isEmpty() )
        {
            logger.warn( "Metrics extension reporting is not configured. Please configure one of the available exporting options to be able to use metrics. " +
                    "Metrics extension is disabled." );
            //TODO: we need to carry this knowledge to all database level extentions that will be registered later on.
            life.clear();
        }

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
}
