/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.harness.junit.rule.EnterpriseNeo4jRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.server.WebContainerTestUtils;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_filename;

public class BoltQueryLoggingMemoryIT
{
    @Rule
    public final Neo4jRule neo4j;

    private Path tmpDir;

    public BoltQueryLoggingMemoryIT() throws IOException
    {
        tmpDir = WebContainerTestUtils.createTempDir().toPath().toAbsolutePath();
        this.neo4j = new EnterpriseNeo4jRule()
                .withConfig( GraphDatabaseSettings.auth_enabled, false )
                .withConfig( GraphDatabaseSettings.track_query_allocation, true )
                .withConfig( GraphDatabaseSettings.logs_directory, tmpDir )
                .withConfig( GraphDatabaseSettings.log_queries, LogQueryLevel.VERBOSE )
                .withConfig( GraphDatabaseSettings.log_queries_allocation_logging_enabled, true )
                .withConfig( GraphDatabaseSettings.log_queries_runtime_logging_enabled, true )
                .withConfig( GraphDatabaseSettings.log_queries_heap_dump_enabled, true );
    }

    @After
    public void cleanUp()
    {
        try
        {
            FileUtils.deleteRecursively( tmpDir.toFile() );
        }
        catch ( IOException e )
        {
            // Too bad, but let us not fail the test over it
        }
    }

    @Test
    public void shouldLogQueryMemoryUsageAndHeapDumpViaBolt() throws IOException
    {
        int numberOfDumpedQueries = 1; // Heap dumps are time consuming, try to keep it to a minimum
        long estimatedHeapUsage = Morsel.createInitialRow().estimatedHeapUsage();

        try ( var driver = graphDatabaseDriver( neo4j.boltURI() );
              var session = driver.session() )
        {
            for ( int i = 0; i < numberOfDumpedQueries; i++ )
            {
                session.run( "RETURN 1 AS num" ).consume();
            }

            var params = new HashMap<String,Object>();
            params.put( "context", NullValue.NULL );

            // We should _not_ heap dump on these kind of queries that we get from internal client tools
            session.run( "CALL dbms.routing.getRoutingTable($context)", params );
            session.run( "CALL db.ping()" );
        }

        int numberOfQueries = numberOfDumpedQueries + 2;

        Path queriesLog = neo4j.config().get( log_queries_filename );
        List<String> lines = Files.readAllLines( queriesLog );
        assertThat( lines ).hasSize( numberOfQueries * 2 );
        int lineNumber = 0;
        List<Integer> queryIds = new ArrayList<>();
        for ( String line : lines )
        {
            assertThat( line ).contains( "INFO" );
            assertThat( line ).contains( "bolt-session" );
            assertThat( line ).contains( "neo4j-java" );
            assertThat( line ).contains( "client/127.0.0.1:" );
            assertThat( line ).contains( "server/127.0.0.1:" + neo4j.boltURI().getPort() );
            if ( lineNumber < numberOfDumpedQueries * 2 )
            {
                assertThat( line ).contains( " - RETURN 1 AS num - {} - runtime=pipelined - {}" );
            }
            if ( (lineNumber & 1) == 1 )
            {
                assertThat( line ).contains( String.format( ": %s B -", estimatedHeapUsage ) ); // Estimated memory usage
                // Extract the query id
                int queryIdIndex = line.indexOf( "id:" );
                assertThat( queryIdIndex >= 0 );
                int queryId = Integer.parseInt( line.substring( queryIdIndex + 3, queryIdIndex + 4 ) );
                queryIds.add( queryId );
            }
            lineNumber++;
        }
        assertThat( queryIds ).hasSize( numberOfQueries );
        int i = 0;
        for ( int queryId : queryIds )
        {
            // We should have gotten a heap dump per query that we do not filter out
            Path heapDump = queriesLog.resolveSibling( String.format( "query-%s.hprof", queryId ) );
            if ( i < numberOfDumpedQueries )
            {
                assertThat( Files.exists( heapDump ) );
            }
            else
            {
                assertThat( Files.notExists( heapDump ) );
            }
            i++;
        }
    }
}
