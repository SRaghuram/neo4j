/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyMap;
import static org.apache.commons.io.IOUtils.readLines;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_filename;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.AuthSubject.AUTH_DISABLED;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

@TestDirectoryExtension
class QueryLoggerIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementServiceBuilder databaseBuilder;
    private static final String QUERY = "CREATE (n:Foo {bar: 'baz'})";

    private File logsDirectory;
    private File logFilename;
    private GraphDatabaseFacade systemDb;
    private GraphDatabaseFacade database;
    private DatabaseManagementService databaseManagementService;

    @BeforeEach
    void setUp()
    {
        logsDirectory = new File( testDirectory.homeDir(), "logs" );
        logFilename = new File( logsDirectory, "query.log" );
        databaseBuilder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) );
    }

    @AfterEach
    void tearDown()
    {
        if ( database != null )
        {
            databaseManagementService.shutdown();
        }
    }

    @Test
    void shouldLogSystemCommandOnlyOnce() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.auth_enabled, true );
        buildDatabase();

        // WHEN
        executeSystemCommandSuperUser( "CREATE USER foo SET PASSWORD 'neo4j' CHANGE NOT REQUIRED" );
        executeSystemCommandSuperUser( "CREATE OR REPLACE USER foo SET PASSWORD 'neo5j' CHANGE NOT REQUIRED" );
        executeSystemCommandSuperUser( "CREATE USER foo IF NOT EXISTS SET PASSWORD 'neo4j' CHANGE NOT REQUIRED" );
        executeSystemCommandSuperUser( "CREATE ROLE role IF NOT EXISTS" );
        executeSystemCommandSuperUser( "GRANT ROLE role TO foo" );
        executeSystemCommandSuperUser( "GRANT ACCESS ON DATABASE * TO role" );
        executeSystemCommandSuperUser( "SHOW USERS" );
        executeSystemCommandSuperUser( "SHOW DATABASES" );
        executeSystemCommandSuperUser( "DROP USER foo" );
        executeSystemCommandSuperUser( "DROP ROLE role" );

        databaseManagementService.shutdown();

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines.size(), equalTo( 10 ) );
        String connectionDetails = connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME );
        assertThat( logLines, contains(
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails,
                        "CREATE USER foo SET PASSWORD '******' CHANGE NOT REQUIRED" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails,
                        "CREATE OR REPLACE USER foo SET PASSWORD '******' CHANGE NOT REQUIRED" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails,
                        "CREATE USER foo IF NOT EXISTS SET PASSWORD '******' CHANGE NOT REQUIRED" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails, "CREATE ROLE role IF NOT EXISTS" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails, "GRANT ROLE role TO foo" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails, "GRANT ACCESS ON DATABASE * TO role" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails, "SHOW USERS" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails, "SHOW DATABASES" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails, "DROP USER foo" ) ),
                endsWith( String.format( "%s - %s - {} - runtime=system - {}", connectionDetails, "DROP ROLE role" ) )
        ) );
    }

    @Test
    void shouldLogCustomUserName() throws Throwable
    {
        // turn on query logging
        databaseBuilder.setConfig( auth_enabled, true )
                       .setConfig( logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( log_queries, LogQueryLevel.INFO );
        buildDatabase();

        // create users
        executeSystemCommandSuperUser( "CREATE USER mats SET PASSWORD 'neo4j' CHANGE NOT REQUIRED" );
        executeSystemCommandSuperUser( "CREATE USER andres SET PASSWORD 'neo4j' CHANGE NOT REQUIRED" );
        executeSystemCommandSuperUser( "GRANT ROLE architect TO mats" );
        executeSystemCommandSuperUser( "GRANT ROLE reader TO andres" );
        EnterpriseLoginContext mats = login( "mats", "neo4j" );

        // run query
        executeQuery( mats, "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})", emptyMap() );
        executeQuery( mats, "CREATE (:Label)", emptyMap() );

        // switch user, run query
        EnterpriseLoginContext andres = login( "andres", "neo4j" );
        executeQuery( andres, "MATCH (n:Label) RETURN n", emptyMap() );

        databaseManagementService.shutdown();

        // THEN
        List<String> logLines = readAllUserQueryLines( logFilename );

        assertThat( logLines, hasSize( 3 ) );
        assertThat( logLines.get( 0 ), containsString( "mats" ) );
        assertThat( logLines.get( 1 ), containsString( "mats" ) );
        assertThat( logLines.get( 2 ), containsString( "andres" ) );
    }

    @Test
    void shouldLogTXMetaDataInQueryLog() throws Throwable
    {
        // turn on query logging
        databaseBuilder.setConfig( logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( auth_enabled, true );
        buildDatabase();

        executeSystemCommandSuperUser( "ALTER USER neo4j SET PASSWORD '123' CHANGE NOT REQUIRED" );

        EnterpriseLoginContext subject = login( "neo4j", "123" );
        executeQuery( subject, "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})", emptyMap() );

        // Set meta data and execute query in transaction
        try ( InternalTransaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, subject ) )
        {
            tx.execute( "CALL tx.setMetaData( { User: 'Johan' } )", emptyMap() );
            tx.execute( "CALL dbms.procedures() YIELD name RETURN name", emptyMap() ).close();
            tx.execute( "MATCH (n) RETURN n", emptyMap() ).close();
            tx.execute( QUERY, emptyMap() );
            tx.commit();
        }

        // Ensure that old meta data is not retained
        try ( InternalTransaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, subject ) )
        {
            tx.execute( "CALL tx.setMetaData( { Location: 'Sweden' } )", emptyMap() );
            tx.execute( "MATCH ()-[r]-() RETURN count(r)", emptyMap() ).close();
            tx.commit();
        }

        databaseManagementService.shutdown();

        // THEN
        List<String> logLines = readAllUserQueryLines( logFilename );

        assertThat( logLines, hasSize( 7 ) );
        assertThat( logLines.get( 0 ), not( containsString( "User: 'Johan'" ) ) );
        // we don't care if setTXMetaData contains the meta data
        //assertThat( logLines.get( 1 ), containsString( "User: Johan" ) );
        assertThat( logLines.get( 2 ), containsString( "User: 'Johan'" ) );
        assertThat( logLines.get( 3 ), containsString( "User: 'Johan'" ) );
        assertThat( logLines.get( 4 ), containsString( "User: 'Johan'" ) );

        // we want to make sure that the new transaction does not carry old meta data
        assertThat( logLines.get( 5 ), not( containsString( "User: 'Johan'" ) ) );
        assertThat( logLines.get( 6 ), containsString( "Location: 'Sweden'" ) );
    }

    @Test
    void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false );
        buildDatabase();

        executeQuery( QUERY );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format( " B - %s - %s - runtime=slotted - {}", connectionAndDatabaseDetails(), QUERY ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    void shouldLogQueryStart() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false );
        buildDatabase();

        executeQuery( QUERY );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 2, logLines.size() );
        assertThat( logLines.get( 0 ), containsString( "Query started:" ) );
        assertThat( logLines.get( 0 ), containsString( " 0 B -" ) );
    }

    @Test
    void shouldIgnoreThreshold() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false )
                       .setConfig( GraphDatabaseSettings.log_queries_threshold, Duration.ofSeconds( 6 ) );
        buildDatabase();

        executeQuery( QUERY );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 2, logLines.size() );
        assertThat( logLines.get( 0 ), containsString( "Query started:" ) );
    }

    @Test
    void shouldObeyLevelChangeDuringRuntime() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false );
        buildDatabase();

        executeQuery( QUERY );
        executeQuery( "call dbms.setConfigValue('" + GraphDatabaseSettings.log_queries.name() + "', 'verbose')" );
        executeQuery( QUERY );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.stream().filter( line -> line.contains( "Query started:" ) ).count() );
    }

    @Test
    void shouldLogQueryWhenItFailsToParse() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() );
        buildDatabase();

        assertThrows( QueryExecutionException.class, () -> executeQuery( "Not a parsable query" ) );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines.get( 0 ), containsString( "ERROR" ) );
        assertThat( logLines.get( 0 ), containsString( "Not a parsable query" ) );
    }

    @Test
    void shouldLogCorrectPlanningTimeWhenFailsToParse() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled, true );
        buildDatabase();

        assertThrows( QueryExecutionException.class, () -> executeQuery( "Not a parsable query" ) );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), containsString( "ERROR" ) );
        assertThat( logLines.get( 0 ), containsString( "Not a parsable query" ) );
        assertFalse( logLines.get( 0 ).split( "planning: " )[1].startsWith( "-" ),
                "Expected log to contain positive planning time, was:\n" + logLines.get( 0 ) );
    }

    @Test
    void shouldLogStartBeforeQueryParsingIfRawLoggingEnabled() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false )
                .setConfig( GraphDatabaseSettings.log_queries_early_raw_logging_enabled, true );
        buildDatabase();

        assertThrows( QueryExecutionException.class, () -> executeQuery( "Not a parsable query" ) );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 2, logLines.size() );
        assertThat( logLines.get( 0 ), containsString( "Query started:" ) );
    }

    @Test
    void shouldLogPasswordsIfRawLoggingEnabled() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_early_raw_logging_enabled, true );
        buildDatabase();

        executeSystemCommandSuperUser( "CREATE USER testUser SET PASSWORD 'hello'" );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 2, logLines.size() );
        assertThat( logLines.get( 0 ), containsString( "Query started:" ) );
        assertThat( logLines.get( 0 ), containsString( "CREATE USER testUser SET PASSWORD 'hello'" ) );
    }

    @Test
    void shouldNotLogPasswordsWhenSystemCommandFails() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() );
        buildDatabase();

        // "This is an administration command and it should be executed against the system database: CREATE USER"
        assertThrows( QueryExecutionException.class, () -> executeQuery( "CREATE USER testUser SET PASSWORD 'hello'" ) );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines.get( 0 ), containsString( "CREATE USER testUser SET PASSWORD '******'" ) );
    }

    @Test
    void shouldLogParametersWhenNestedMap() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, true );
        buildDatabase();

        Map<String,Object> props = new LinkedHashMap<>(); // to be sure about ordering in the last assertion
        props.put( "name", "Roland" );
        props.put( "position", "Gunslinger" );
        props.put( "followers", Arrays.asList( "Jake", "Eddie", "Susannah" ) );

        Map<String,Object> params = new HashMap<>();
        params.put( "props", props );

        String query = "CREATE ($props)";
        executeQuery( query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format(
                " B - %s - %s - {props: {name: 'Roland', position: 'Gunslinger', followers: ['Jake', 'Eddie', 'Susannah']}}"
                        + " - runtime=slotted - {}",
                connectionAndDatabaseDetails(),
                query ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    void shouldLogNoSuchProcedureInQueryLog() throws Throwable
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                       .setConfig( GraphDatabaseSettings.log_queries_early_raw_logging_enabled, true )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() );
        buildDatabase();

        try
        {
            executeQuery( "Call this.procedure.doesnt.exist()" );
            fail( "Should have thrown" );
        }
        catch ( QueryExecutionException e )
        {
            databaseManagementService.shutdown();
        }

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertEquals( 2, logLines.size() );
        String expectedMessage = "There is no procedure with the name `this.procedure.doesnt.exist` registered for this database instance.";
        assertEquals( 1, logLines.stream().filter( line -> line.contains( expectedMessage ) ).count() );
    }

    @Test
    void shouldLogRuntime() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_runtime_logging_enabled, true );
        buildDatabase();

        String query = "CYPHER runtime=slotted RETURN 42";
        executeQuery( query );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format(
                " B - %s - %s - {} - runtime=slotted - {}",
                connectionAndDatabaseDetails(),
                query ) ) );
    }

    @Test
    void shouldLogParametersWhenList() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() );
        buildDatabase();

        Map<String,Object> params = new HashMap<>();
        params.put( "ids", Arrays.asList( 0, 1, 2 ) );
        String query = "MATCH (n) WHERE id(n) in $ids RETURN n.name";
        executeQuery( query, params );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ),
                endsWith( String.format( " B - %s - %s - {ids: [0, 1, 2]} - runtime=pipelined - {}", connectionAndDatabaseDetails(), query ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    void disabledQueryLogging()
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.OFF )
                       .setConfig( log_queries_filename, logFilename.toPath().toAbsolutePath() );
        buildDatabase();

        executeQuery( QUERY );
        databaseManagementService.shutdown();

        assertFalse( fileSystem.fileExists( logFilename ) );
    }

    @Test
    void disabledQueryLogRotation() throws Exception
    {
        final File logsDirectory = new File( testDirectory.homeDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        final File shiftedLogFilename1 = new File( logsDirectory, "query.log.1" );
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_rotation_threshold, 0L );
        buildDatabase();

        // Logging is done asynchronously, so write many times to make sure we would have rotated something
        for ( int i = 0; i < 100; i++ )
        {
            executeQuery( QUERY );
        }

        databaseManagementService.shutdown();

        assertFalse( shiftedLogFilename1.exists(), "There should not exist a shifted log file because rotation is disabled" );

        List<String> lines = readAllLines( logFilename );
        assertEquals( 100, lines.size() );
    }

    @Test
    void queryLogRotation()
    {
        final File logsDirectory = new File( testDirectory.homeDir(), "logs" );
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.log_queries_max_archives, 100 )
                       .setConfig( GraphDatabaseSettings.log_queries_rotation_threshold, 1L );
        buildDatabase();

        // Logging is done asynchronously, and it turns out it's really hard to make it all work the same on Linux
        // and on Windows, so just write many times to make sure we rotate several times.

        for ( int i = 0; i < 100; i++ )
        {
            executeQuery( QUERY );
        }

        databaseManagementService.shutdown();

        File[] queryLogs = fileSystem.listFiles( logsDirectory, ( dir, name ) -> name.startsWith( "query.log" ) );
        assertThat( "Expect to have more then one query log file.", queryLogs.length, greaterThanOrEqualTo( 2 ) );

        List<String> loggedQueries = Arrays.stream( queryLogs )
                                           .map( this::readAllLinesSilent )
                                           .flatMap( Collection::stream )
                                           .collect( Collectors.toList() );
        assertThat( "Expected log file to have at least one log entry", loggedQueries, hasSize( 100 ) );

        buildDatabase();
        try ( Transaction transaction = database.beginTx() )
        {
            // Now modify max_archives and rotation_threshold at runtime, and observe that we end up with fewer larger files
            transaction.execute( "CALL dbms.setConfigValue('" + GraphDatabaseSettings.log_queries_max_archives.name() + "','1')" );
            transaction.execute( "CALL dbms.setConfigValue('" + GraphDatabaseSettings.log_queries_rotation_threshold.name() + "','20m')" );
            for ( int i = 0; i < 100; i++ )
            {
                transaction.execute( QUERY );
            }
            transaction.commit();
        }

        databaseManagementService.shutdown();

        queryLogs = fileSystem.listFiles( logsDirectory, ( dir, name ) -> name.startsWith( "query.log" ) );
        assertThat( "Expect to have more then one query log file.", queryLogs.length, lessThan( 100 ) );

        loggedQueries = Arrays.stream( queryLogs )
                              .map( this::readAllLinesSilent )
                              .flatMap( Collection::stream )
                              .collect( Collectors.toList() );
        assertThat( "Expected log file to have at least one log entry", loggedQueries.size(), lessThanOrEqualTo( 202 ) );
    }

    @Test
    void shouldNotLogPasswordWhenChanging() throws Exception
    {
        databaseBuilder
                .setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, true );
        buildDatabase();

        EnterpriseLoginContext neo = login( "neo4j", "neo4j" );
        executeQueryOnSystem( neo, "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'abc123'", Collections.emptyMap() );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        var lastEntry = logLines.size() - 1;
        var obfuscatedQuery = "ALTER CURRENT USER SET PASSWORD FROM '******' TO '******'";
        assertThat( logLines.get( lastEntry ),
                endsWith( String.format( "%s%s - %s - {} - runtime=system - {}",
                        connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ), "neo4j", obfuscatedQuery ) ) );
    }

    @Test
    void shouldNotLogPasswordWhenChangedByAdmin() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.auth_enabled, true );
        buildDatabase();

        executeSystemCommandSuperUser( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );
        executeSystemCommandSuperUser( "CREATE USER foo SET PASSWORD '123abc'" );

        EnterpriseLoginContext neo = login( "neo4j", "neo4j" );
        executeQueryOnSystem( neo, "ALTER USER foo SET PASSWORD 'abc123'", Collections.emptyMap() );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        var lastEntry = logLines.size() - 1;
        var obfuscatedQuery = "ALTER USER foo SET PASSWORD '******'";
        assertThat( logLines.get( lastEntry ),
                endsWith( String.format( "%s%s - %s - {} - runtime=system - {}",
                        connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ), "neo4j", obfuscatedQuery ) ) );
    }

    @Test
    void shouldNotLogPasswordWhenChangedByAdminProcedure() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.auth_enabled, true );
        buildDatabase();

        executeSystemCommandSuperUser( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );
        executeSystemCommandSuperUser( "CREATE USER foo SET PASSWORD '123abc'" );

        EnterpriseLoginContext neo = login( "neo4j", "neo4j" );
        executeQueryOnSystem( neo, "CALL dbms.security.changeUserPassword('foo', 'abc123')", Collections.emptyMap() );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        var lastEntry = logLines.size() - 1;
        var obfuscatedQuery = "CALL dbms.security.changeUserPassword('foo', '******')";
        assertThat( logLines.get( lastEntry ),
                endsWith( String.format( "%s%s - %s - {} - runtime=system - {}",
                        connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ), "neo4j", obfuscatedQuery ) ) );
    }

    @Test
    void shouldNotLogPasswordForCreate() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( GraphDatabaseSettings.auth_enabled, true );
        buildDatabase();

        executeSystemCommandSuperUser( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );

        EnterpriseLoginContext neo = login( "neo4j", "neo4j" );
        executeQueryOnSystem( neo, "CREATE USER foo SET PASSWORD \"abc123\"", Collections.emptyMap() );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        var lastEntry = logLines.size() - 1;
        var obfuscatedQuery = "CREATE USER foo SET PASSWORD '******'";
        assertThat( logLines.get( lastEntry ),
                endsWith( String.format( "%s%s - %s - {} - runtime=system - {}",
                        connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ), "neo4j", obfuscatedQuery ) ) );
    }

    @Test
    void shouldNotLogPasswordForCreateWithProcedureOnSystem() throws Exception
    {
        databaseBuilder
                .setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, true );
        buildDatabase();

        executeSystemCommandSuperUser( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );

        EnterpriseLoginContext neo = login( "neo4j", "neo4j" );
        executeQueryOnSystem( neo, "CALL dbms.security.createUser('foo', 'abc123')", Collections.emptyMap() );
        databaseManagementService.shutdown();

        List<String> logLines = readAllLines( logFilename );
        var lastEntry = logLines.size() - 1;
        var obfuscatedQuery = "CALL dbms.security.createUser('foo', '******')";
        assertThat( logLines.get( lastEntry ),
                endsWith( String.format( "%s%s - %s - {} - runtime=system - {}",
                        connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ), "neo4j", obfuscatedQuery ) ) );
    }

    @Test
    void canBeEnabledAndDisabledAtRuntime() throws Exception
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.OFF )
                       .setConfig( log_queries_filename, logFilename.toPath().toAbsolutePath() );
        buildDatabase();
        List<String> strings;

        try
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.execute( QUERY ).close();

                // File will not be created until query logService is enabled.
                assertFalse( fileSystem.fileExists( logFilename ) );

                transaction.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'info')" ).close();
                transaction.execute( QUERY ).close();

                // Both config change and query should exist
                strings = readAllLines( logFilename );
                assertEquals( 2, strings.size() );

                transaction.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'off')" ).close();
                transaction.execute( QUERY ).close();
                transaction.commit();
            }
        }
        finally
        {
            databaseManagementService.shutdown();
        }

        // Value should not change when disabled
        strings = readAllLines( logFilename );
        assertEquals( 2, strings.size() );
    }

    @Test
    void logQueriesWithSystemTimeZoneIsConfigured()
    {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try
        {
            TimeZone.setDefault( TimeZone.getTimeZone( ZoneOffset.ofHours( 5 ) ) );
            executeSingleQueryWithTimeZoneLog();
            TimeZone.setDefault( TimeZone.getTimeZone( ZoneOffset.ofHours( -5 ) ) );
            executeSingleQueryWithTimeZoneLog();
            List<String> allQueries = readAllLinesSilent( logFilename );
            assertTrue( allQueries.get( 0 ).contains( "+0500" ) );
            assertTrue( allQueries.get( 1 ).contains( "-0500" ) );
        }
        finally
        {
            TimeZone.setDefault( defaultTimeZone );
        }
    }

    private void executeSingleQueryWithTimeZoneLog()
    {
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                       .setConfig( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM )
                       .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() );
        buildDatabase();
        executeQuery( QUERY );
        databaseManagementService.shutdown();
    }

    void buildDatabase()
    {
        databaseManagementService = databaseBuilder.build();
        database = (GraphDatabaseFacade) databaseManagementService.database( DEFAULT_DATABASE_NAME );
        systemDb = (GraphDatabaseFacade) databaseManagementService.database( SYSTEM_DATABASE_NAME );
    }

    private EnterpriseAuthManager getAuthManager()
    {
        return database.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }

    private static String connectionAndDatabaseDetails()
    {
        return connectionAndDatabaseDetails( DEFAULT_DATABASE_NAME );
    }

    private static String connectionAndDatabaseDetails( String databaseName )
    {
        return EMBEDDED_CONNECTION.asConnectionDetails() + "\t" + databaseName + " - ";
    }

    private List<String> readAllLinesSilent( File logFilename )
    {
        try
        {
            return readAllLines( fileSystem, logFilename );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private List<String> readAllUserQueryLines( File logFilename ) throws IOException
    {
        return readAllLines( logFilename ).stream()
                .filter( l -> !l.contains( connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ) ) )
                .collect( Collectors.toList() );
    }

    private List<String> readAllLines( File logFilename ) throws IOException
    {
        return readAllLines( fileSystem, logFilename );
    }

    private static List<String> readAllLines( FileSystemAbstraction fs, File logFilename ) throws IOException
    {
        // this is needed as the EphemeralFSA is broken, and creates a new file when reading a non-existent file from
        // a valid directory
        if ( !fs.fileExists( logFilename ) )
        {
            throw new FileNotFoundException( "File does not exist." );
        }

        try ( var reader = fs.openAsReader( logFilename, StandardCharsets.UTF_8 ) )
        {
            return readLines( reader );
        }
    }

    private void executeQuery( String query )
    {
        executeQuery( query, Collections.emptyMap() );
    }

    private void executeQuery( String query, Map<String,Object> params )
    {
        executeQuery( EnterpriseLoginContext.AUTH_DISABLED, query, params );
    }

    private void executeQuery( EnterpriseLoginContext loginContext, String call, Map<String,Object> params )
    {
        executeQuery( database, loginContext, call, params );
    }

    private void executeSystemCommandSuperUser( String query )
    {
        executeQueryOnSystem( EnterpriseLoginContext.AUTH_DISABLED, query, Collections.emptyMap() );
    }

    private void executeQueryOnSystem( EnterpriseLoginContext loginContext, String call, Map<String,Object> params )
    {
        executeQuery( systemDb, loginContext, call, params );
    }

    private void executeQuery( GraphDatabaseFacade db, EnterpriseLoginContext loginContext, String query, Map<String,Object> params )
    {
        Consumer<ResourceIterator<Map<String,Object>>> resultConsumer = ResourceIterator::close;
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.IMPLICIT, loginContext ) )
        {
            Map<String,Object> p = (params == null) ? emptyMap() : params;
            resultConsumer.accept( tx.execute( query, p ) );
            tx.commit();
        }
    }

    private EnterpriseLoginContext login( String username, String password ) throws InvalidAuthTokenException
    {
        return getAuthManager().login( authToken( username, password ) );
    }
}
