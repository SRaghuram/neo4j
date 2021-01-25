/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;
import com.neo4j.configuration.MetricsSettings;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;

import static com.neo4j.configuration.MetricsSettings.csv_enabled;
import static com.neo4j.configuration.MetricsSettings.graphite_enabled;
import static com.neo4j.configuration.MetricsSettings.graphite_interval;
import static com.neo4j.configuration.MetricsSettings.graphite_server;
import static com.neo4j.configuration.MetricsSettings.metrics_enabled;
import static com.neo4j.configuration.MetricsSettings.metrics_prefix;
import static com.neo4j.configuration.MetricsSettings.prometheus_enabled;
import static com.neo4j.configuration.MetricsSettings.prometheus_endpoint;
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

    public EventReporterBuilder( Config config, MetricRegistry registry, Log logger, ExtensionContext extensionContext,
            LifeSupport life, FileSystemAbstraction fileSystem, ConnectorPortRegister portRegister )
    {
        this.config = config;
        this.registry = registry;
        this.logger = logger;
        this.extensionContext = extensionContext;
        this.life = life;
        this.fileSystem = fileSystem;
        this.portRegister = portRegister;
    }

    public boolean configure()
    {
        boolean consumersConfigured = false;
        if ( !config.get( metrics_enabled ) )
        {
            return consumersConfigured;
        }

        if ( config.get( csv_enabled ) )
        {
            CsvOutput csvOutput = new CsvOutput( config, registry, logger, extensionContext, fileSystem );
            life.add( csvOutput );
            consumersConfigured = true;
        }

        if ( config.get( graphite_enabled ) )
        {
            SocketAddress server = config.get( graphite_server );
            long period = config.get( graphite_interval ).toMillis();
            GraphiteOutput graphiteOutput = new GraphiteOutput( server, period, registry, logger );
            life.add( graphiteOutput );
            consumersConfigured = true;
        }

        if ( config.get( prometheus_enabled ) )
        {
            SocketAddress server = config.get( prometheus_endpoint );
            PrometheusOutput prometheusOutput = new PrometheusOutput( server, registry, logger, portRegister );
            life.add( prometheusOutput );
            consumersConfigured = true;
        }

        if ( config.get( MetricsSettings.jmx_enabled ) )
        {
            String domain = config.get( metrics_prefix ) + METRICS_JMX_BEAN_SUFFIX;
            JmxReporter jmxReporter = JmxReporter.forRegistry( registry ).inDomain( domain )
                    .createsObjectNamesWith( new MetricsObjectNameFactory() ).build();
            life.add( new JmxOutput( jmxReporter ) );
            consumersConfigured = true;
        }

        return consumersConfigured;
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
