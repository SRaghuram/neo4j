/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.net.InetSocketAddress;

class PrometheusHttpServer extends HTTPServer
{
    PrometheusHttpServer( String host, int port ) throws IOException
    {
        super( host, port, true );
    }

    InetSocketAddress getAddress()
    {
        return server.getAddress();
    }
}
