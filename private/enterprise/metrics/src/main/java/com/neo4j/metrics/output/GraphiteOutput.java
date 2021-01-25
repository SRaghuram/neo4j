/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

public class GraphiteOutput implements Lifecycle
{
    private final SocketAddress socketAddress;
    private final long period;
    private final MetricRegistry registry;
    private final Log logger;

    private GraphiteReporter graphiteReporter;

    GraphiteOutput( SocketAddress socketAddress, long period, MetricRegistry registry, Log logger )
    {
        this.socketAddress = socketAddress;
        this.period = period;
        this.registry = registry;
        this.logger = logger;
    }

    @Override
    public void init()
    {
        // Setup Graphite reporting
        final Graphite graphite = new Graphite( socketAddress.getHostname(), socketAddress.getPort() );

        graphiteReporter = GraphiteReporter.forRegistry( registry )
                .convertRatesTo( TimeUnit.SECONDS )
                .convertDurationsTo( TimeUnit.MILLISECONDS )
                .filter( MetricFilter.ALL )
                .build( graphite );
    }

    @Override
    public void start()
    {
        graphiteReporter.start( period, TimeUnit.MILLISECONDS );
        logger.info( "Sending metrics to Graphite server at " + socketAddress );
    }

    @Override
    public void stop()
    {
        graphiteReporter.close();
    }

    @Override
    public void shutdown()
    {
        graphiteReporter = null;
    }
}
