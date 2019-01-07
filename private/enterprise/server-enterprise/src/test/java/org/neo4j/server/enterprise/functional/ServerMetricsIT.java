/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.enterprise.functional;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.metrics.MetricsSettings;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

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
        NeoServer server = EnterpriseServerBuilder.serverOnRandomPorts()
                .usingDataDir( directory.databaseDir().getAbsolutePath() )
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

            String host = "http://localhost:" + server.baseUri().getPort() +
                          ServerSettings.rest_api_path.getDefaultValue() + "/transaction/commit";

            for ( int i = 0; i < 5; i++ )
            {
                ClientResponse r = Client.create().resource( host ).accept( APPLICATION_JSON ).type( APPLICATION_JSON )
                        .post( ClientResponse.class, "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" );
                assertEquals( 200, r.getStatus() );
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
