/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.MetricsSettings.prometheus_enabled;
import static com.neo4j.configuration.MetricsSettings.prometheus_endpoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.PortUtils.getConnectorAddress;

@TestDirectoryExtension
class PrometheusOutputIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() ).setConfig( prometheus_enabled, true )
                .setConfig( prometheus_endpoint, new SocketAddress( "localhost", 0 ) ).build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void httpEndpointShouldBeAvailableAndResponsive() throws IOException
    {
        String url = "http://" + getConnectorAddress( (GraphDatabaseAPI) database, "prometheus" ) + "/metrics";
        URLConnection connection = new URL( url ).openConnection();
        connection.setDoOutput( true );
        connection.connect();
        Scanner s = new Scanner( connection.getInputStream(), StandardCharsets.UTF_8 ).useDelimiter( "\\A" );

        assertTrue( s.hasNext() );
        String response = s.next();
        assertThat( response ).contains( "neo4j.neo4j.ids_in_use.node" );
        assertThat( response ).contains( "neo4j.neo4j.ids_in_use.relationship_type" );
    }
}
