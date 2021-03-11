/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.configuration.MetricsSettings;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory.Dependencies;
import com.neo4j.metrics.metric.MetricsRegister;
import com.neo4j.metrics.output.EventReporterBuilder;
import com.neo4j.metrics.output.RotatableCsvReporter;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.log4j.RotatingLogFileWriter;

import static com.neo4j.configuration.MetricsSettings.csv_enabled;
import static com.neo4j.configuration.MetricsSettings.csv_path;
import static com.neo4j.configuration.MetricsSettings.metrics_enabled;
import static com.neo4j.configuration.MetricsSettings.metrics_filter;
import static com.neo4j.configuration.MetricsSettings.metrics_namespaces_enabled;
import static com.neo4j.metrics.database.DatabaseMetricsExporter.databaseMetricsPrefix;

public class GlobalMetricsExtension implements Lifecycle, MetricsManager
{
    private final LifeSupport life = new LifeSupport();
    private final Log logger;
    private final MetricsRegister metricsRegister;
    private final ExtensionContext context;
    private final GlobalMetricsExtensionFactory.Dependencies dependencies;
    private final boolean configured;
    private final DatabaseEventListeners databaseEventListeners;
    private final MetricsFileCleaner metricsFileCleaner;

    public GlobalMetricsExtension( ExtensionContext context, Dependencies dependencies )
    {
        LogService logService = dependencies.logService();
        Config config = dependencies.configuration();
        FileSystemAbstraction fileSystemAbstraction = dependencies.fileSystemAbstraction();
        this.context = context;
        this.dependencies = dependencies;
        this.logger = logService.getUserLog( getClass() );
        MetricRegistry registry = new MetricRegistry();
        RotatableCsvReporter csvReporter = new RotatableCsvReporter( registry, fileSystemAbstraction, config.get( csv_path ),
                config.get( MetricsSettings.csv_rotation_threshold ), config.get( MetricsSettings.csv_max_archives ),
                config.get( MetricsSettings.csv_archives_compression ), RotatingLogFileWriter::new, logger );
        this.metricsRegister = new MetricsRegister( registry, config.get( metrics_filter ) );
        this.metricsFileCleaner = new MetricsFileCleaner( logger, config, csvReporter );
        this.configured =
                new EventReporterBuilder( config, registry, logger, life, fileSystemAbstraction, dependencies.portRegister(), csvReporter ).configure();
        this.databaseEventListeners = dependencies.databaseEventListeners();
    }

    @Override
    public void init()
    {
        databaseEventListeners.registerDatabaseEventListener( metricsFileCleaner );

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
        new GlobalMetricsExporter( metricsRegister, config, context, dependencies, life ).export();
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
        databaseEventListeners.unregisterDatabaseEventListener( metricsFileCleaner );
    }

    @Override
    public MetricsRegister getRegistry()
    {
        return metricsRegister;
    }

    @Override
    public boolean isConfigured()
    {
        return configured;
    }

    private static final class MetricsFileCleaner extends DatabaseEventListenerAdapter
    {
        private final Log logger;
        private final Config config;
        private final RotatableCsvReporter csvReporter;

        private MetricsFileCleaner( Log logger, Config config, RotatableCsvReporter csvReporter )
        {
            this.logger = logger;
            this.config = config;
            this.csvReporter = csvReporter;
        }

        @Override
        public void databaseDrop( DatabaseEventContext eventContext )
        {
            if ( !config.get( metrics_namespaces_enabled ) )
            {
                // Here we might delete metrics that are actually NOT part of the database since there are collisions if the namespace is not enabled
                if ( config.get( csv_enabled ) )
                {
                    logger.warn( "Unable to remove database specific metric files because metric namespaces are not enabled. In order to remove this " +
                            "warning you have set '" + metrics_namespaces_enabled.name() + "=true' in your configuration file." );
                }
                return;
            }
            csvReporter.deleteAll( databaseMetricsPrefix( config, eventContext.getDatabaseName() ) );
        }
    }
}
