/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.graphiteEnabled;
import static org.neo4j.metrics.MetricsSettings.graphiteInterval;
import static org.neo4j.metrics.MetricsSettings.graphiteServer;
import static org.neo4j.metrics.MetricsSettings.metricsPrefix;
import static org.neo4j.metrics.MetricsSettings.prometheusEnabled;
import static org.neo4j.metrics.MetricsSettings.prometheusEndpoint;

public class EventReporterBuilder
{
    private static final String METRICS_JMX_BEAN_SUFFIX = ".metrics";

    private final Config config;
    private final MetricRegistry registry;
    private final Log logger;
    private final ExtensionContext extensionContext;
    private final LifeSupport life;
    private final ConnectorPortRegister portRegister;
    private final FileSystemAbstraction fileSystem;
    private final JobScheduler scheduler;

    public EventReporterBuilder( Config config, MetricRegistry registry, Log logger, ExtensionContext extensionContext,
            LifeSupport life, FileSystemAbstraction fileSystem, JobScheduler scheduler, ConnectorPortRegister portRegister )
    {
        this.config = config;
        this.registry = registry;
        this.logger = logger;
        this.extensionContext = extensionContext;
        this.life = life;
        this.fileSystem = fileSystem;
        this.scheduler = scheduler;
        this.portRegister = portRegister;
    }

    public CompositeEventReporter build()
    {
        CompositeEventReporter reporter = new CompositeEventReporter();
        final String prefix = createMetricsPrefix( config );
        if ( config.get( csvEnabled ) )
        {
            CsvOutput csvOutput = new CsvOutput( config, registry, logger, extensionContext, fileSystem, scheduler );
            reporter.add( csvOutput );
            life.add( csvOutput );
        }

        if ( config.get( graphiteEnabled ) )
        {
            HostnamePort server = config.get( graphiteServer );
            long period = config.get( graphiteInterval ).toMillis();
            GraphiteOutput graphiteOutput = new GraphiteOutput( server, period, registry, logger, prefix );
            reporter.add( graphiteOutput );
            life.add( graphiteOutput );
        }

        if ( config.get( prometheusEnabled ) )
        {
            HostnamePort server = config.get( prometheusEndpoint );
            PrometheusOutput prometheusOutput = new PrometheusOutput( server, registry, logger, portRegister );
            reporter.add( prometheusOutput );
            life.add( prometheusOutput );
        }

        if ( config.get( MetricsSettings.jmxEnabled ) )
        {
            JmxReporter jmxReporter = JmxReporter.forRegistry( registry ).inDomain( prefix + METRICS_JMX_BEAN_SUFFIX ).build();
            life.add( new JmxOutput( jmxReporter ) );
        }

        return reporter;
    }

    private String createMetricsPrefix( Config config )
    {
        return config.get( metricsPrefix );
    }
}
