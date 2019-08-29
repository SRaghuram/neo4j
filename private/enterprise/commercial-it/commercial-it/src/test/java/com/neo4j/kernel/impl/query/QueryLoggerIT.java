/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;
import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.server.security.enterprise.auth.CommercialAuthAndUserManager;
import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_filename;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.AuthSubject.AUTH_DISABLED;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

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
    private GraphDatabaseFacade db;
    private GraphDatabaseService database;
    private DatabaseManagementService databaseManagementService;
    private DatabaseManagementService dbManagementService;

    @BeforeEach
    void setUp()
    {
        logsDirectory = new File( testDirectory.storeDir(), "logs" );
        logFilename = new File( logsDirectory, "query.log" );
        AssertableLogProvider inMemoryLog = new AssertableLogProvider();
        databaseBuilder = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .setInternalLogProvider( inMemoryLog )
                .impermanent();
    }

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            dbManagementService.shutdown();
        }
        if ( database != null )
        {
            databaseManagementService.shutdown();
        }
    }

    @Test
    void shouldLogCustomUserName() throws Throwable
    {
        // turn on query logging
        databaseBuilder.setConfig( auth_enabled, true )
                       .setConfig( logs_directory, logsDirectory.toPath().toAbsolutePath() )
                       .setConfig( log_queries, LogQueryLevel.INFO );
        dbManagementService = databaseBuilder.build();
        db = (GraphDatabaseFacade) dbManagementService.database( DEFAULT_DATABASE_NAME );
        EnterpriseUserManager userManager = getUserManager();
        // create users
        userManager.newUser( "mats", password( "neo4j" ), false );
        userManager.newUser( "andres", password( "neo4j" ), false );
        userManager.addRoleToUser( "architect", "mats" );
        userManager.addRoleToUser( "reader", "andres" );

        CommercialLoginContext mats = login( "mats", "neo4j" );

        // run query
        executeQuery( mats, "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})", emptyMap() );
        executeQuery( mats, "CREATE (:Label)", emptyMap() );

        // switch user, run query
        CommercialLoginContext andres = login( "andres", "neo4j" );
        executeQuery( andres, "MATCH (n:Label) RETURN n", emptyMap() );

        dbManagementService.shutdown();

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
        databaseBuilder.setConfig( logs_directory, logsDirectory.toPath().toAbsolutePath() );
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO );
        databaseBuilder.setConfig( auth_enabled, true );
        dbManagementService = databaseBuilder.build();
        db = (GraphDatabaseFacade) dbManagementService.database( DEFAULT_DATABASE_NAME );

        getUserManager().setUserPassword( "neo4j", password( "123" ), false );

        CommercialLoginContext subject = login( "neo4j", "123" );
        executeQuery( subject, "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})", emptyMap() );

        // Set meta data and execute query in transaction
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.explicit, subject ) )
        {
            tx.execute( "CALL dbms.setTXMetaData( { User: 'Johan' } )", emptyMap() );
            tx.execute( "CALL dbms.procedures() YIELD name RETURN name", emptyMap() ).close();
            tx.execute( "MATCH (n) RETURN n", emptyMap() ).close();
            tx.execute( QUERY, emptyMap() );
            tx.commit();
        }

        // Ensure that old meta data is not retained
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.explicit, subject ) )
        {
            tx.execute( "CALL dbms.setTXMetaData( { Location: 'Sweden' } )", emptyMap() );
            tx.execute( "MATCH ()-[r]-() RETURN count(r)", emptyMap() ).close();
            tx.commit();
        }

        dbManagementService.shutdown();

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
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false ).build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        executeQueryAndShutdown( database );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format( " ms: %s - %s - {}", connectionAndDatabaseDetails(), QUERY ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    void shouldLogQueryStart() throws Exception
    {
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false )
                .build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        executeQueryAndShutdown( database );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 2, logLines.size() );
        assertThat( logLines.get( 0 ), containsString( "Query started:" ) );
    }

    @Test
    void shouldIgnoreThreshold() throws Exception
    {
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.VERBOSE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false )
                .setConfig( GraphDatabaseSettings.log_queries_threshold, Duration.ofSeconds( 6 ) )
                .build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        executeQueryAndShutdown( database );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 2, logLines.size() );
        assertThat( logLines.get( 0 ), containsString( "Query started:" ) );
    }

    @Test
    void shouldObeyLevelChangeDuringRuntime() throws Exception
    {
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false )
                .build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        executeQueryAndShutdown( database );
        executeQueryAndShutdown( database, "call dbms.setConfigValue('" + GraphDatabaseSettings.log_queries.name() + "', 'verbose')", emptyMap() );
        executeQueryAndShutdown( database );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.stream().filter( line -> line.contains( "Query started:" ) ).count() );
    }

    @Test
    void shouldLogParametersWhenNestedMap() throws Exception
    {
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, true ).build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        Map<String,Object> props = new LinkedHashMap<>(); // to be sure about ordering in the last assertion
        props.put( "name", "Roland" );
        props.put( "position", "Gunslinger" );
        props.put( "followers", Arrays.asList( "Jake", "Eddie", "Susannah" ) );

        Map<String,Object> params = new HashMap<>();
        params.put( "props", props );

        String query = "CREATE ($props)";
        executeQueryAndShutdown( database, query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format(
                " ms: %s - %s - {props: {name: 'Roland', position: 'Gunslinger', followers: ['Jake', 'Eddie', 'Susannah']}}"
                        + " - {}",
                connectionAndDatabaseDetails(),
                query ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    void shouldLogRuntime() throws Exception
    {
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_runtime_logging_enabled, true ).build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        String query = "CYPHER runtime=slotted RETURN 42";
        executeQueryAndShutdown( database, query, emptyMap() );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format(
                " ms: %s - %s - {} - runtime=slotted - {}",
                connectionAndDatabaseDetails(),
                query ) ) );
    }

    @Test
    void shouldLogParametersWhenList() throws Exception
    {
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() ).build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        Map<String,Object> params = new HashMap<>();
        params.put( "ids", Arrays.asList( 0, 1, 2 ) );
        String query = "MATCH (n) WHERE id(n) in $ids RETURN n.name";
        executeQueryAndShutdown( database, query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ),
                endsWith( String.format( " ms: %s - %s - {ids: [0, 1, 2]} - {}", connectionAndDatabaseDetails(), query ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    void disabledQueryLogging()
    {
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.OFF )
                .setConfig( log_queries_filename, logFilename.toPath().toAbsolutePath() ).build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        executeQueryAndShutdown( database );

        assertFalse( fileSystem.fileExists( logFilename ) );
    }

    @Test
    void disabledQueryLogRotation() throws Exception
    {
        final File logsDirectory = new File( testDirectory.storeDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        final File shiftedLogFilename1 = new File( logsDirectory, "query.log.1" );
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_rotation_threshold, 0L ).build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        // Logging is done asynchronously, so write many times to make sure we would have rotated something
        for ( int i = 0; i < 100; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.execute( QUERY );
                transaction.commit();
            }
        }

        databaseManagementService.shutdown();

        assertFalse( shiftedLogFilename1.exists(), "There should not exist a shifted log file because rotation is disabled" );

        List<String> lines = readAllLines( logFilename );
        assertEquals( 100, lines.size() );
    }

    @Test
    void queryLogRotation()
    {
        final File logsDirectory = new File( testDirectory.storeDir(), "logs" );
        databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.log_queries_max_archives, 100 )
                .setConfig( GraphDatabaseSettings.log_queries_rotation_threshold, 1L );
        databaseManagementService = databaseBuilder.build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );

        // Logging is done asynchronously, and it turns out it's really hard to make it all work the same on Linux
        // and on Windows, so just write many times to make sure we rotate several times.

        for ( int i = 0; i < 100; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.execute( QUERY );
                transaction.commit();
            }
        }

        databaseManagementService.shutdown();

        File[] queryLogs = fileSystem.listFiles( logsDirectory, ( dir, name ) -> name.startsWith( "query.log" ) );
        assertThat( "Expect to have more then one query log file.", queryLogs.length, greaterThanOrEqualTo( 2 ) );

        List<String> loggedQueries = Arrays.stream( queryLogs )
                                           .map( this::readAllLinesSilent )
                                           .flatMap( Collection::stream )
                                           .collect( Collectors.toList() );
        assertThat( "Expected log file to have at least one log entry", loggedQueries, hasSize( 100 ) );

        databaseManagementService = databaseBuilder.build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );
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
        databaseManagementService = databaseBuilder
                .setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, true ).build();
        database = databaseManagementService.database( SYSTEM_DATABASE_NAME );
        GraphDatabaseFacade facade = (GraphDatabaseFacade) this.database;

        CommercialAuthManager authManager = facade.getDependencyResolver().resolveDependency( CommercialAuthManager.class );
        CommercialLoginContext neo = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );

        String query = "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'abc123'";
        try ( InternalTransaction tx = facade.beginTransaction( KernelTransaction.Type.explicit, neo ) )
        {
            Result res = tx.execute( query );
            res.close();
            tx.commit();
        }
        finally
        {
            databaseManagementService.shutdown();
        }

        List<String> logLines = readAllLines( logFilename );
        var lastEntry = logLines.size() - 1;
        var obfuscatedQuery = "ALTER CURRENT USER SET PASSWORD FROM '******' TO '******'";
        assertThat( logLines.get( lastEntry ),
                endsWith( String.format( " ms: %s%s - %s - {} - {}", connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ), "neo4j", obfuscatedQuery ) ) );
    }

    @Test
    void shouldNotLogPasswordWhenChangedByAdmin() throws Exception
    {
        databaseManagementService = databaseBuilder
                .setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, true ).build();
        database = databaseManagementService.database( SYSTEM_DATABASE_NAME );
        GraphDatabaseFacade facade = (GraphDatabaseFacade) this.database;

        executeSystemCommandSuperUser( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );
        executeSystemCommandSuperUser( "CREATE USER foo SET PASSWORD '123abc'" );

        CommercialAuthManager authManager = facade.getDependencyResolver().resolveDependency( CommercialAuthManager.class );
        CommercialLoginContext neo = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );

        try ( InternalTransaction tx = facade.beginTransaction( KernelTransaction.Type.explicit, neo ) )
        {
            Result res = tx.execute( "ALTER USER foo SET PASSWORD 'abc123'" );
            res.close();
            tx.commit();
        }
        finally
        {
            databaseManagementService.shutdown();
        }

        List<String> logLines = readAllLines( logFilename );
        var lastEntry = logLines.size() - 1;
        var obfuscatedQuery = "ALTER USER foo SET PASSWORD '******'";
        assertThat( logLines.get( lastEntry ),
                endsWith( String.format( " ms: %s%s - %s - {} - {}", connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ), "neo4j", obfuscatedQuery ) ) );
    }

    @Test
    void shouldNotLogPasswordForCreate() throws Exception
    {
        databaseManagementService = databaseBuilder
                .setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, true ).build();
        database = databaseManagementService.database( SYSTEM_DATABASE_NAME );
        GraphDatabaseFacade facade = (GraphDatabaseFacade) this.database;

        executeSystemCommandSuperUser( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );

        CommercialAuthManager authManager = facade.getDependencyResolver().resolveDependency( CommercialAuthManager.class );
        CommercialLoginContext neo = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );

        try ( InternalTransaction tx = facade.beginTransaction( KernelTransaction.Type.explicit, neo ) )
        {
            Result res = tx.execute( "CREATE USER foo SET PASSWORD \"abc123\"" );
            res.close();
            tx.commit();
        }
        finally
        {
            databaseManagementService.shutdown();
        }

        List<String> logLines = readAllLines( logFilename );
        var lastEntry = logLines.size() - 1;
        var obfuscatedQuery = "CREATE USER foo SET PASSWORD '******'";
        assertThat( logLines.get( lastEntry ),
                endsWith( String.format( " ms: %s%s - %s - {} - {}", connectionAndDatabaseDetails( SYSTEM_DATABASE_NAME ), "neo4j", obfuscatedQuery ) ) );
    }

    @Test
    void canBeEnabledAndDisabledAtRuntime() throws Exception
    {
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.OFF )
                .setConfig( log_queries_filename, logFilename.toPath().toAbsolutePath() ).build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );
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
        databaseManagementService = databaseBuilder.setConfig( log_queries, LogQueryLevel.INFO )
                .setConfig( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.toPath().toAbsolutePath() ).build();
        database = databaseManagementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.execute( QUERY ).close();
            transaction.commit();
        }
        databaseManagementService.shutdown();
    }

    private static void executeQueryAndShutdown( GraphDatabaseService database )
    {
        executeQueryAndShutdown( database, QUERY, emptyMap() );
    }

    private static void executeQueryAndShutdown( GraphDatabaseService database, String query, Map<String,Object> params )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Result execute = transaction.execute( query, params );
            execute.close();
            transaction.commit();
        }
    }

    private EnterpriseUserManager getUserManager()
    {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        CommercialAuthAndUserManager commercialAuthManager = dependencyResolver.resolveDependency( CommercialAuthAndUserManager.class );
        return commercialAuthManager.getUserManager();
    }

    private CommercialAuthManager getAuthManager()
    {
        return db.getDependencyResolver().resolveDependency( CommercialAuthManager.class );
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
        List<String> logLines = new ArrayList<>();
        // this is needed as the EphemeralFSA is broken, and creates a new file when reading a non-existent file from
        // a valid directory
        if ( !fs.fileExists( logFilename ) )
        {
            throw new FileNotFoundException( "File does not exist." );
        }

        try ( BufferedReader reader = new BufferedReader(
                fs.openAsReader( logFilename, StandardCharsets.UTF_8 ) ) )
        {
            for ( String line; ( line = reader.readLine() ) != null; )
            {
                logLines.add( line );
            }
        }
        return logLines;
    }

    private void executeQuery( CommercialLoginContext loginContext, String call, Map<String,Object> params )
    {
        Consumer<ResourceIterator<Map<String,Object>>> resultConsumer = ResourceIterator::close;
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.implicit, loginContext ) )
        {
            Map<String,Object> p = (params == null) ? emptyMap() : params;
            resultConsumer.accept( tx.execute( call, p ) );
            tx.commit();
        }
    }

    private void executeSystemCommandSuperUser( String query )
    {
        GraphDatabaseFacade facade = (GraphDatabaseFacade) databaseManagementService.database( SYSTEM_DATABASE_NAME );

        Consumer<ResourceIterator<Map<String,Object>>> resultConsumer = ResourceIterator::close;
        try ( InternalTransaction tx = facade.beginTransaction( KernelTransaction.Type.implicit, LoginContext.AUTH_DISABLED ) )
        {
            resultConsumer.accept( tx.execute( query, emptyMap() ) );
            tx.commit();
        }
    }

    private CommercialLoginContext login( String username, String password ) throws InvalidAuthTokenException
    {
        return getAuthManager().login( authToken( username, password ) );
    }
}
