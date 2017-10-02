/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.enterprise.functional;

import java.io.File;
import java.io.IOException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.source.server.ServerMetrics;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.enterprise.helpers.CommercialServerBuilder;
import org.neo4j.test.rule.SuppressOutput;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;

public class ServerMetricsIT
{
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldShowServerMetrics() throws Throwable
    {
        // Given
        String path = folder.getRoot().getAbsolutePath();
        File metricsPath = new File( path + "/metrics" );
        NeoServer server = CommercialServerBuilder.serverOnRandomPorts()
                .usingDataDir( path )
                .withProperty( MetricsSettings.metricsEnabled.name(), "true" )
                .withProperty( MetricsSettings.csvEnabled.name(), "true" )
                .withProperty( MetricsSettings.csvPath.name(), metricsPath.getPath() )
                .withProperty( MetricsSettings.csvInterval.name(), "100ms" )
                .persistent()
                .build();
        try
        {
            // when
            server.start();

            String host = "http://localhost:" + server.baseUri().getPort() +
                          ServerSettings.rest_api_path.getDefaultValue() + "/transaction/commit";

            for ( int i = 0; i < 5; i++ )
            {
                ClientResponse r = Client.create().resource( host ).accept( APPLICATION_JSON ).type( APPLICATION_JSON )
                        .post( ClientResponse.class, "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" );
                assertEquals( 200, r.getStatus() );
            }

            // then
            assertMetricsExists( metricsPath, ServerMetrics.THREAD_JETTY_ALL );
            assertMetricsExists( metricsPath, ServerMetrics.THREAD_JETTY_IDLE );
        }
        finally
        {
            server.stop();
        }
    }

    private void assertMetricsExists( File metricsPath, String meticsName ) throws IOException, InterruptedException
    {
        File file = metricsCsv( metricsPath, meticsName );
        long threadCount = readLongValue( file );
        assertThat( threadCount, greaterThan( 0L ) );
    }
}
