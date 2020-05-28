/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

/**
 * Prometheus poll data from clients, this exposes a HTTP endpoint at a configurable port.
 */
public class PrometheusOutput implements Lifecycle, EventReporter
{
    protected PrometheusHttpServer server;
    private final SocketAddress socketAddress;
    private final MetricRegistry registry;
    private final Log logger;
    private final ConnectorPortRegister portRegister;
    private final Map<String,Object> registeredEvents = new ConcurrentHashMap<>();
    private final MetricRegistry eventRegistry;

    PrometheusOutput( SocketAddress socketAddress, MetricRegistry registry, Log logger, ConnectorPortRegister portRegister )
    {
        this.socketAddress = socketAddress;
        this.registry = registry;
        this.logger = logger;
        this.portRegister = portRegister;
        this.eventRegistry = new MetricRegistry();
    }

    @Override
    public void init()
    {
        // Setup prometheus collector
        CollectorRegistry.defaultRegistry.register( new DropwizardExports( registry ) );

        // We have to have a separate registry to not pollute the default one
        CollectorRegistry.defaultRegistry.register( new DropwizardExports( eventRegistry ) );
    }

    @Override
    public void start() throws Exception
    {
        if ( server == null )
        {
            server = new PrometheusHttpServer( socketAddress.getHostname(), socketAddress.getPort() );
            portRegister.register( "prometheus", server.getAddress() );
            logger.info( "Started publishing Prometheus metrics at http://" + server.getAddress() + "/metrics" );
        }
    }

    @Override
    public void stop()
    {
        if ( server != null )
        {
            final String address = server.getAddress().toString();
            server.stop();
            server = null;
            logger.info( "Stopped Prometheus endpoint at http://" + address + "/metrics" );
        }
    }

    @Override
    public void shutdown()
    {
        this.stop();
    }

    @Override
    public void report( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters, SortedMap<String,Timer> timers )
    {
        if ( !gauges.isEmpty() )
        {
            String gaugeKey = gauges.firstKey();
            if ( !registeredEvents.containsKey( gaugeKey ) )
            {
                eventRegistry.register( gaugeKey, (Gauge) () -> registeredEvents.get( gaugeKey ) );
            }
            registeredEvents.put( gaugeKey, gauges.get( gaugeKey ).getValue() );
        }

        if ( !meters.isEmpty() )
        {
            String meterKey = meters.firstKey();
            if ( !registeredEvents.containsKey( meterKey ) )
            {
                eventRegistry.register( meterKey, new Counter()
                {
                    @Override
                    public long getCount()
                    {
                        return ((Number) registeredEvents.get( meterKey )).longValue();
                    }
                } );
            }
            registeredEvents.put( meterKey, meters.get( meterKey ).getCount() );
        }
    }
}
