/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent;

import com.neo4j.bench.model.model.Neo4jConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileExists;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileNotEmpty;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class AgentTestHelper
{
    static final String BOLT_LISTEN_ADDRESS = "dbms.connector.bolt.listen_address";
    static final String HTTP_LISTEN_ADDRESS = "dbms.connector.http.listen_address";

    private static Config driverConfig = Config.builder()
                                               .withLogging( Logging.none() )
                                               .build();

    static void alterDatabase( URI boltUri )
    {
        withDriver( boltUri, session -> session.run( "CREATE (:foo {id:0});" ).consume() );
    }

    static void assertDatabaseIsAvailable( URI boltUri )
    {
        withDriver( boltUri, session ->
                assertEquals( 0, session.run( "MATCH (n) RETURN count(n) AS i" ).single().get( "i", -1 ) ) );
    }

    private static void withDriver( URI boltUri, Consumer<Session> action )
    {
        try ( Driver driver = GraphDatabase.driver( boltUri,
                                                    AuthTokens.none(),
                                                    driverConfig );
              Session session = driver.session() )
        {
            action.accept( session );
        }
    }

    static void assertDatabaseIsNotAvailable( URI boltUri )
    {
        try
        {
            assertDatabaseIsAvailable( boltUri );
            fail( "Driver should not be able to connect" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    static void assertAgentUninitialized( AgentState agentState )
    {
        assertAgentState( agentState, AgentState.State.UNINITIALIZED, null, null, null, null, null );
    }

    static void assertAgentInitialized( AgentState agentState, String packageName, String datasetName )
    {
        assertAgentState( agentState, AgentState.State.INITIALIZED, packageName, datasetName, null, null, null );
    }

    static void assertAgentStarted( AgentState agentState, String packageName, String datasetName, Boolean copyStore, Neo4jConfig neo4jConfig, URI boltUri )
    {
        assertAgentState( agentState, AgentState.State.STARTED, packageName, datasetName, copyStore, neo4jConfig, boltUri );
    }

    private static void assertAgentState( AgentState agentState,
                                          AgentState.State state,
                                          String packageName,
                                          String datasetName,
                                          Boolean copyStore,
                                          Neo4jConfig neo4jConfig,
                                          URI boltUri )
    {
        assertThat( agentState.state(), equalTo( state ) );
        assertThat( agentState.agentStartedAt(), notNullValue() );
        assertThat( agentState.productName(), packageName == null ? nullValue() : equalTo( packageName ) );
        assertThat( agentState.datasetName(), datasetName == null ? nullValue() : equalTo( datasetName ) );
        assertThat( agentState.databaseStartedAt(), state == AgentState.State.STARTED ? notNullValue() : nullValue() );
        assertThat( agentState.copyStore(), copyStore == null ? nullValue() : equalTo( copyStore ) );
        assertThat( agentState.databaseConfig(), neo4jConfig == null ? nullValue() : equalTo( neo4jConfig ) );
        assertThat( agentState.boltUri(), boltUri == null ? nullValue() : equalTo( boltUri ) );
    }

    static void assertSingleStore( Path workspace, String dataset )
    {
        assertThat( countStoresOrStoreCopies( workspace, dataset ), equalTo( 1L ) );
    }

    private static long countStoresOrStoreCopies( Path workspace, String dataset )
    {
        try ( Stream<Path> list = Files.list( workspace ) )
        {
            return list.filter( file -> file.getFileName().toString().startsWith( dataset ) ).count();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    static void assertArchiveContent( Path resultDirectory )
    {
        assertFileNotEmpty( resultDirectory.resolve( "neo4j.conf" ) );
        assertFileNotEmpty( resultDirectory.resolve( "debug.log" ) );
        assertFileNotEmpty( resultDirectory.resolve( "neo4j.log" ) );
        assertFileNotEmpty( resultDirectory.resolve( "neo4j-out.log" ) );
        assertFileExists( resultDirectory.resolve( "neo4j-error.log" ) );
    }

    static void assertLogLineInFileWithTextExist( Path file, String content ) throws IOException
    {
        String regExp = ".*" + content + ".*";
        List<String> allLines = file == null ? emptyList() : Files.readAllLines( file );
        long filteredLogLinesCount = allLines.stream().filter( line -> line.matches( regExp ) ).count();
        assertThat( filteredLogLinesCount, greaterThan( 0L ) );
    }

    static void assertEnvironmentVariables()
    {
        assertThat( "Unpacked neo4j package is needed for test at env.NEO4J_DIR", System.getenv( "NEO4J_DIR" ), notNullValue() );
        assertThat( "env.JAVA_HOME must be set for test", System.getenv( "JAVA_HOME" ), notNullValue() );
    }
}
