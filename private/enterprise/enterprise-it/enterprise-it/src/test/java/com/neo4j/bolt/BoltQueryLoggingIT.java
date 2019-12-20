/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.server.WebContainerTestUtils;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_filename;

public class BoltQueryLoggingIT
{
    @Rule
    public final Neo4jRule neo4j;

    public BoltQueryLoggingIT() throws IOException
    {
        Path tmpDir = WebContainerTestUtils.createTempDir().toPath().toAbsolutePath();
        this.neo4j = new Neo4jRule()
                .withConfig( GraphDatabaseSettings.auth_enabled, false )
                .withConfig( GraphDatabaseSettings.logs_directory, tmpDir )
                .withConfig( GraphDatabaseSettings.log_queries, LogQueryLevel.INFO );
    }

    @Test
    public void shouldLogQueriesViaBolt() throws IOException
    {
        try ( var driver = graphDatabaseDriver( neo4j.boltURI() );
              var session = driver.session() )
        {
            for ( int i = 0; i < 5; i++ )
            {
                session.run( "RETURN 1 AS num" ).consume();
            }
        }

        Path queriesLog = neo4j.config().get( log_queries_filename );
        List<String> lines = Files.readAllLines( queriesLog );
        assertThat( lines, hasSize( 5 ) );
        for ( String line : lines )
        {
            assertThat( line, containsString( "INFO" ) );
            assertThat( line, containsString( "bolt-session" ) );
            assertThat( line, containsString( "neo4j-java" ) );
            assertThat( line, containsString( "client/127.0.0.1:" ) );
            assertThat( line, containsString( "server/127.0.0.1:" + neo4j.boltURI().getPort() ) );
            assertThat( line, containsString( " - RETURN 1 AS num - {} - {}" ) );
        }
    }
}
