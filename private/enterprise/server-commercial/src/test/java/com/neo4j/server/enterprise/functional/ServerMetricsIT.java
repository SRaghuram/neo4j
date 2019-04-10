/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.functional;

import com.neo4j.server.enterprise.helpers.CommercialServerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import org.neo4j.metrics.MetricsSettings;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class ServerMetricsIT
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldShowServerMetrics() throws Throwable
    {
        // Given
        File metrics = directory.file( "metrics" );
        NeoServer server = CommercialServerBuilder.serverOnRandomPorts()
                .usingDataDir( directory.storeDir().getAbsolutePath() )
                .withProperty( MetricsSettings.metricsEnabled.name(), "true" )
                .withProperty( MetricsSettings.csvEnabled.name(), "true" )
                .withProperty( MetricsSettings.csvPath.name(), metrics.getPath() )
                .withProperty( MetricsSettings.csvInterval.name(), "100ms" )
                .persistent()
                .build();
        try
        {
            // when
            server.start();

            String endpoint = "http://localhost:" + server.baseUri().getPort() +
                              ServerSettings.rest_api_path.getDefaultValue() + "/transaction/commit";

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder( URI.create( endpoint ) )
                    .header( ACCEPT, APPLICATION_JSON )
                    .header( CONTENT_TYPE, APPLICATION_JSON )
                    .POST( ofString( "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" ) )
                    .build();

            for ( int i = 0; i < 5; i++ )
            {
                HttpResponse<Void> response = client.send( request, discarding() );
                assertEquals( 200, response.statusCode() );
            }

            // then
            assertMetricsExists( metrics, "neo4j.server.threads.jetty.all" );
            assertMetricsExists( metrics, "neo4j.server.threads.jetty.idle" );
        }
        finally
        {
            server.stop();
        }
    }

    private static void assertMetricsExists( File metricsPath, String metricsName ) throws InterruptedException
    {
        File file = metricsCsv( metricsPath, metricsName );
        assertEventually( () -> threadCountReader( file ), greaterThan( 0L ), 1, TimeUnit.MINUTES );
    }

    private static Long threadCountReader( File file ) throws InterruptedException
    {
        try
        {
            return readLongGaugeValue( file );
        }
        catch ( IOException io )
        {
            throw new UncheckedIOException( io );
        }
    }
}
