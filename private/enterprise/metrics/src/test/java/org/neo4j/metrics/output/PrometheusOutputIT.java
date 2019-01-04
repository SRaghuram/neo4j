/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.output;

import com.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.metrics.MetricsSettings.prometheusEnabled;
import static org.neo4j.metrics.MetricsSettings.prometheusEndpoint;
import static org.neo4j.metrics.source.db.EntityCountMetrics.COUNTS_NODE;
import static org.neo4j.metrics.source.db.EntityCountMetrics.COUNTS_RELATIONSHIP_TYPE;
import static org.neo4j.test.PortUtils.getConnectorAddress;

@ExtendWith( TestDirectoryExtension.class )
class PrometheusOutputIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.storeDir() )
                .setConfig( prometheusEnabled, Settings.TRUE )
                .setConfig( prometheusEndpoint, "localhost:0" )
                .newGraphDatabase();
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void httpEndpointShouldBeAvailableAndResponsive() throws IOException
    {
        String url = "http://" + getConnectorAddress( (GraphDatabaseAPI) database, "prometheus" ) + "/metrics";
        URLConnection connection = new URL( url ).openConnection();
        connection.setDoOutput( true );
        connection.connect();
        Scanner s = new Scanner( connection.getInputStream(), "UTF-8" ).useDelimiter( "\\A" );

        assertTrue( s.hasNext() );
        String response = s.next();
        assertTrue( response.contains( COUNTS_NODE ) );
        assertTrue( response.contains( COUNTS_RELATIONSHIP_TYPE ) );
    }
}
