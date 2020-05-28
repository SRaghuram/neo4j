/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.function.LongConsumer;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Log;

import static java.util.Collections.emptySortedMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class PrometheusOutputTest
{
    @Test
    void eventsShouldBeRedirectedToGauges() throws Throwable
    {
        MetricRegistry registry = new MetricRegistry();
        DynamicAddressPrometheusOutput dynamicOutput = new DynamicAddressPrometheusOutput( "localhost", registry, mock( Log.class ) );

        LongConsumer callback = l ->
        {
            TreeMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( "my.event", () -> l );
            dynamicOutput.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };

        callback.accept( 10 );

        dynamicOutput.init();
        dynamicOutput.start();

        String serverAddress = dynamicOutput.getServerAddress();
        assertTrue( getResponse( serverAddress ).contains( "my_event 10.0" ) );
        assertTrue( getResponse( serverAddress ).contains( "my_event 10.0" ) );

        callback.accept( 20 );
        assertTrue( getResponse( serverAddress ).contains( "my_event 20.0" ) );
        assertTrue( getResponse( serverAddress ).contains( "my_event 20.0" ) );

        dynamicOutput.shutdown();
    }

    @Test
    void metricsRegisteredAfterStartShouldBeIncluded() throws Throwable
    {
        MetricRegistry registry = new MetricRegistry();
        DynamicAddressPrometheusOutput dynamicOutput = new DynamicAddressPrometheusOutput( "localhost", registry, mock( Log.class ) );

        LongConsumer callback = l ->
        {
            TreeMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( "my.event", () -> l );
            dynamicOutput.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };

        registry.register( "my.metric", (Gauge) () -> 10 );

        dynamicOutput.init();
        dynamicOutput.start();

        callback.accept( 20 );

        String serverAddress = dynamicOutput.getServerAddress();
        String response = getResponse( serverAddress );
        assertTrue( response.contains( "my_metric 10.0" ) );
        assertTrue( response.contains( "my_event 20.0" ) );

        dynamicOutput.shutdown();
    }

    private static String getResponse( String serverAddress ) throws IOException
    {
        String url = "http://" + serverAddress + "/metrics";
        URLConnection connection = new URL( url ).openConnection();
        connection.setDoOutput( true );
        connection.connect();
        try ( Scanner s = new Scanner( connection.getInputStream(), StandardCharsets.UTF_8 ).useDelimiter( "\\A" ) )
        {
            assertTrue( s.hasNext() );
            String ret = s.next();
            assertFalse( s.hasNext() );
            return ret;
        }
    }

    private static class DynamicAddressPrometheusOutput extends PrometheusOutput
    {
        DynamicAddressPrometheusOutput( String host, MetricRegistry registry, Log logger )
        {
            super( new SocketAddress( host, 0 ), registry, logger, mock( ConnectorPortRegister.class ) );
        }

        String getServerAddress()
        {
            InetSocketAddress address = server.getAddress();
            return address.getHostString() + ":" + address.getPort();
        }
    }
}
