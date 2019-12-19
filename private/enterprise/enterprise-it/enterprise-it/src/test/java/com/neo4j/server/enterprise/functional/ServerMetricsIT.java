/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.functional;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static com.neo4j.server.enterprise.helpers.EnterpriseWebContainerBuilder.builderOnRandomPorts;
import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class ServerMetricsIT
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldShowServerMetrics() throws Throwable
    {
        // Given
        File metrics = directory.file( "metrics" );
        var webServerContainer = builderOnRandomPorts()
                                                  .usingDataDir( directory.homeDir().getAbsolutePath() )
                                                  .withProperty( MetricsSettings.metricsEnabled.name(), TRUE )
                                                  .withProperty( MetricsSettings.csvEnabled.name(), TRUE )
                                                  .withProperty( MetricsSettings.csvPath.name(), metrics.getPath() )
                                                  .withProperty( MetricsSettings.csvInterval.name(), "100ms" )
                                                  .persistent()
                                                  .build();
        try
        {
            // when

            String endpoint = "http://localhost:" + webServerContainer.getBaseUri().getPort() +
                              ServerSettings.db_api_path.defaultValue() + "/neo4j/tx/commit";

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
            webServerContainer.shutdown();
        }
    }

    private static void assertMetricsExists( File metricsPath, String metricsName )
    {
        File file = metricsCsv( metricsPath, metricsName );
        assertEventually( () -> threadCountReader( file ), greaterThan( 0L ), 1, TimeUnit.MINUTES );
    }

    private static Long threadCountReader( File file )
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
