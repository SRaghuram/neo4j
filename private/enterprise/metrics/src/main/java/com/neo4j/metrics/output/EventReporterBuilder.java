/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;
import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.csvEnabled;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.graphiteEnabled;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.graphiteInterval;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.graphiteServer;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.metricsEnabled;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.metricsPrefix;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.prometheusEnabled;
import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.prometheusEndpoint;
import static javax.management.ObjectName.quote;

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
        if ( !config.get( metricsEnabled ) )
        {
            return reporter;
        }

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
            GraphiteOutput graphiteOutput = new GraphiteOutput( server, period, registry, logger );
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
            String domain = config.get( metricsPrefix ) + METRICS_JMX_BEAN_SUFFIX;
            JmxReporter jmxReporter = JmxReporter.forRegistry( registry ).inDomain( domain )
                    .createsObjectNamesWith( new MetricsObjectNameFactory() ).build();
            life.add( new JmxOutput( jmxReporter ) );
        }

        return reporter;
    }

    private static class MetricsObjectNameFactory implements ObjectNameFactory
    {
        private static final String NAME = "name";

        @Override
        public ObjectName createName( String type, String domain, String name )
        {
            try
            {
                ObjectName objectName = new ObjectName( domain, NAME, name );
                String validatedName = objectName.isPropertyValuePattern() ? quote( name ) : name;
                return new ObjectName( domain, NAME, validatedName );
            }
            catch ( MalformedObjectNameException e )
            {
                try
                {
                    return new ObjectName( domain, NAME, quote( name ) );
                }
                catch ( MalformedObjectNameException ne )
                {
                    throw new RuntimeException( ne );
                }
            }
        }
    }
}
