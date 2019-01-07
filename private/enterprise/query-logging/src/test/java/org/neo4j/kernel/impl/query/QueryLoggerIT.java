/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.query;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.server.security.enterprise.auth.EmbeddedInteraction;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.virtual.VirtualValues;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.log_queries;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.log_queries_max_archives;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.log_queries_rotation_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.internal.kernel.api.security.AuthSubject.AUTH_DISABLED;
import static org.neo4j.server.security.auth.BasicAuthManagerTest.password;

public class QueryLoggerIT
{

    // It is imperative that this test executes using a real filesystem; otherwise rotation failures will not be
    // detected on Windows.
    @Rule
    public final DefaultFileSystemRule fileSystem = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private GraphDatabaseBuilder databaseBuilder;
    private static final String QUERY = "CREATE (n:Foo {bar: 'baz'})";

    private File logsDirectory;
    private File logFilename;
    private EmbeddedInteraction db;
    private GraphDatabaseService database;

    @Before
    public void setUp()
    {
        logsDirectory = new File( testDirectory.storeDir(), "logs" );
        logFilename = new File( logsDirectory, "query.log" );
        AssertableLogProvider inMemoryLog = new AssertableLogProvider();
        databaseBuilder = new TestCommercialGraphDatabaseFactory()
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem.get() ) )
                .setInternalLogProvider( inMemoryLog )
                .newImpermanentDatabaseBuilder( testDirectory.databaseDir() );
    }

    @After
    public void tearDown()
    {
        if ( db != null )
        {
            db.tearDown();
        }
        if ( database != null )
        {
            database.shutdown();
        }
    }

    @Test
    public void shouldLogCustomUserName() throws Throwable
    {
        // turn on query logging
        final Map<String, String> config = stringMap(
            logs_directory.name(), logsDirectory.getPath(),
            log_queries.name(), Settings.TRUE );
        db = new EmbeddedInteraction( databaseBuilder, config );

        // create users
        db.getLocalUserManager().newUser( "mats", password( "neo4j" ), false );
        db.getLocalUserManager().newUser( "andres", password( "neo4j" ), false );
        db.getLocalUserManager().addRoleToUser( "architect", "mats" );
        db.getLocalUserManager().addRoleToUser( "reader", "andres" );

        EnterpriseLoginContext mats = db.login( "mats", "neo4j" );

        // run query
        String unwindQuery = "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})";
        db.executeQuery( mats, unwindQuery, Collections.emptyMap(), ResourceIterator::close );
        db.executeQuery( mats, "CREATE (:Label)", Collections.emptyMap(), ResourceIterator::close );

        // switch user, run query
        EnterpriseLoginContext andres = db.login( "andres", "neo4j" );
        String matchQuery = "MATCH (n:Label) RETURN n";
        db.executeQuery( andres, matchQuery, Collections.emptyMap(), ResourceIterator::close );

        db.tearDown();

        // THEN
        List<String> logLines = readAllLines( logFilename );

        int line1 = findLogLineThatContains( unwindQuery, logLines );
        assertNotEquals( "Expected a log line containing '" + unwindQuery + "'", line1, NOT_FOUND );
        assertThat( logLines.get( line1 ), containsString( "mats" ) );
        assertThat( logLines.get( line1 + 1 ), containsString( "mats" ) );

        int line2 = findLogLineThatContains( matchQuery, logLines );
        assertNotEquals( "Expected a log line containing '" + matchQuery + "'", line2, NOT_FOUND );
        assertThat( logLines.get( line2 ), containsString( "andres" ) );
    }

    @Test
    public void shouldLogTXMetaDataInQueryLog() throws Throwable
    {
        // turn on query logging
        databaseBuilder.setConfig( logs_directory, logsDirectory.getPath() );
        databaseBuilder.setConfig( log_queries, Settings.TRUE );
        db = new EmbeddedInteraction( databaseBuilder, Collections.emptyMap() );
        GraphDatabaseFacade graph = db.getLocalGraph();

        db.getLocalUserManager().setUserPassword( "neo4j", password( "123" ), false );

        EnterpriseLoginContext subject = db.login( "neo4j", "123" );
        db.executeQuery( subject, "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})", Collections.emptyMap(),
                ResourceIterator::close );

        // Set meta data and execute query in transaction
        try ( InternalTransaction tx = db.beginLocalTransactionAsUser( subject, KernelTransaction.Type.explicit ) )
        {
            graph.execute( "CALL dbms.setTXMetaData( { User: 'Johan' } )", Collections.emptyMap() );
            graph.execute( "CALL dbms.procedures() YIELD name RETURN name", Collections.emptyMap() ).close();
            graph.execute( "MATCH (n) RETURN n", Collections.emptyMap() ).close();
            graph.execute( QUERY, Collections.emptyMap() );
            tx.success();
        }

        // Ensure that old meta data is not retained
        try ( InternalTransaction tx = db.beginLocalTransactionAsUser( subject, KernelTransaction.Type.explicit ) )
        {
            graph.execute( "CALL dbms.setTXMetaData( { Location: 'Sweden' } )", Collections.emptyMap() );
            graph.execute( "MATCH ()-[r]-() RETURN count(r)", Collections.emptyMap() ).close();
            tx.success();
        }

        db.tearDown();

        // THEN
        List<String> logLines = readAllLines( logFilename );

        String expectedMessage = "UNWIND range(0, 10) AS i";
        int line = findLogLineThatContains( expectedMessage, logLines );

        assertNotEquals( "Expected a log line containing '" + expectedMessage + "'", line, NOT_FOUND );
        assertEquals( line + 7, logLines.size() );
        assertThat( logLines.get( line ), not( containsString( "User: 'Johan'" ) ) );
        // we don't care if setTXMetaData contains the meta data
        //assertThat( logLines.get( line + 1 ), containsString( "User: Johan" ) );
        assertThat( logLines.get( line + 2 ), containsString( "User: 'Johan'" ) );
        assertThat( logLines.get( line + 3 ), containsString( "User: 'Johan'" ) );
        assertThat( logLines.get( line + 4 ), containsString( "User: 'Johan'" ) );

        // we want to make sure that the new transaction does not carry old meta data
        assertThat( logLines.get( line + 5 ), not( containsString( "User: 'Johan'" ) ) );
        assertThat( logLines.get( line + 6 ), containsString( "Location: 'Sweden'" ) );
    }

    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        database = databaseBuilder.setConfig( log_queries, Settings.TRUE )
                .setConfig( logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, Settings.FALSE )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format( " ms: %s - %s - {}", clientConnectionInfo(), QUERY ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    public void shouldLogParametersWhenNestedMap() throws Exception
    {
        database = databaseBuilder.setConfig( log_queries, Settings.TRUE )
                .setConfig( logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, Settings.TRUE )
                .newGraphDatabase();

        Map<String,Object> props = new LinkedHashMap<>(); // to be sure about ordering in the last assertion
        props.put( "name", "Roland" );
        props.put( "position", "Gunslinger" );
        props.put( "followers", Arrays.asList( "Jake", "Eddie", "Susannah" ) );

        Map<String,Object> params = new HashMap<>();
        params.put( "props", props );

        String query = "CREATE ({props})";
        executeQueryAndShutdown( database, query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format(
                " ms: %s - %s - {props: {name: 'Roland', position: 'Gunslinger', followers: ['Jake', 'Eddie', 'Susannah']}}"
                        + " - {}",
                clientConnectionInfo(),
                query ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    public void shouldLogRuntime() throws Exception
    {
        database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_runtime_logging_enabled, Settings.TRUE )
                .newGraphDatabase();

        String query = "RETURN 42";
        executeQueryAndShutdown( database, query, Collections.emptyMap() );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format(
                " ms: %s - %s - {} - runtime=interpreted - {}",
                clientConnectionInfo(),
                query ) ) );
    }

    @Test
    public void shouldLogParametersWhenList() throws Exception
    {
        database = databaseBuilder.setConfig( log_queries, Settings.TRUE )
                .setConfig( logs_directory, logsDirectory.getPath() )
                .newGraphDatabase();

        Map<String,Object> params = new HashMap<>();
        params.put( "ids", Arrays.asList( 0, 1, 2 ) );
        String query = "MATCH (n) WHERE id(n) in {ids} RETURN n.name";
        executeQueryAndShutdown( database, query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ),
                endsWith( String.format( " ms: %s - %s - {ids: [0, 1, 2]} - {}", clientConnectionInfo(), query ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    public void disabledQueryLogging()
    {
        database = databaseBuilder.setConfig( log_queries, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.log_queries_filename, logFilename.getPath() )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        assertFalse( fileSystem.fileExists( logFilename ) );
    }

    @Test
    public void disabledQueryLogRotation() throws Exception
    {
        final File logsDirectory = new File( testDirectory.storeDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        final File shiftedLogFilename1 = new File( logsDirectory, "query.log.1" );
        database = databaseBuilder.setConfig( log_queries, Settings.TRUE )
                .setConfig( logs_directory, logsDirectory.getPath() )
                .setConfig( log_queries_rotation_threshold, "0" )
                .newGraphDatabase();

        // Logging is done asynchronously, so write many times to make sure we would have rotated something
        for ( int i = 0; i < 100; i++ )
        {
            database.execute( QUERY );
        }

        database.shutdown();

        assertFalse( "There should not exist a shifted log file because rotation is disabled",
                shiftedLogFilename1.exists() );

        List<String> lines = readAllLines( logFilename );
        assertEquals( 100, lines.size() );
    }

    @Test
    public void queryLogRotation()
    {
        final File logsDirectory = new File( testDirectory.storeDir(), "logs" );
        databaseBuilder.setConfig( log_queries, Settings.TRUE )
                .setConfig( logs_directory, logsDirectory.getPath() )
                .setConfig( log_queries_max_archives, "100" )
                .setConfig( log_queries_rotation_threshold, "1" );
        database = databaseBuilder.newGraphDatabase();

        // Logging is done asynchronously, and it turns out it's really hard to make it all work the same on Linux
        // and on Windows, so just write many times to make sure we rotate several times.

        for ( int i = 0; i < 100; i++ )
        {
            database.execute( QUERY );
        }

        database.shutdown();

        File[] queryLogs = fileSystem.get().listFiles( logsDirectory, ( dir, name ) -> name.startsWith( "query.log" ) );
        assertThat( "Expect to have more then one query log file.", queryLogs.length, greaterThanOrEqualTo( 2 ) );

        List<String> loggedQueries = Arrays.stream( queryLogs )
                                           .map( this::readAllLinesSilent )
                                           .flatMap( Collection::stream )
                                           .collect( Collectors.toList() );
        assertThat( "Expected log file to have at least one log entry", loggedQueries, hasSize( 100 ) );

        database = databaseBuilder.newGraphDatabase();
        // Now modify max_archives and rotation_threshold at runtime, and observe that we end up with fewer larger files
        database.execute( "CALL dbms.setConfigValue('" + log_queries_max_archives.name() + "','1')" );
        database.execute( "CALL dbms.setConfigValue('" + log_queries_rotation_threshold.name() + "','20m')" );
        for ( int i = 0; i < 100; i++ )
        {
            database.execute( QUERY );
        }

        database.shutdown();

        queryLogs = fileSystem.get().listFiles( logsDirectory, ( dir, name ) -> name.startsWith( "query.log" ) );
        assertThat( "Expect to have more then one query log file.", queryLogs.length, lessThan( 100 ) );

        loggedQueries = Arrays.stream( queryLogs )
                              .map( this::readAllLinesSilent )
                              .flatMap( Collection::stream )
                              .collect( Collectors.toList() );
        assertThat( "Expected log file to have at least one log entry", loggedQueries.size(), lessThanOrEqualTo( 202 ) );
    }

    @Test
    public void shouldNotLogPassword() throws Exception
    {
        database = databaseBuilder
                .setConfig( log_queries, Settings.TRUE )
                .setConfig( logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Settings.TRUE )
                .newGraphDatabase();
        GraphDatabaseFacade facade = (GraphDatabaseFacade) this.database;

        EnterpriseAuthManager authManager = facade.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
        EnterpriseLoginContext neo = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );

        String query = "CALL dbms.security.changePassword('abc123')";
        try ( InternalTransaction tx = facade.beginTransaction( KernelTransaction.Type.explicit, neo ) )
        {
            Result res = facade.execute( tx, query, VirtualValues.EMPTY_MAP );
            res.close();
            tx.success();
        }
        finally
        {
            facade.shutdown();
        }

        List<String> logLines = readAllLines( logFilename );

        String expectedMessage = "CALL dbms.security.changePassword(******)";
        int line = findLogLineThatContains( expectedMessage, logLines );

        assertNotEquals( "Expected a log line containing '" + expectedMessage + "'", line, NOT_FOUND );
        assertEquals( line + 1, logLines.size() );
        assertThat( logLines.get( line ), not( containsString( "abc123" ) ) );
        assertThat( logLines.get( line ), containsString( neo.subject().username() ) );
    }

    @Test
    public void canBeEnabledAndDisabledAtRuntime() throws Exception
    {
        database = databaseBuilder.setConfig( log_queries, Settings.FALSE ).setConfig( GraphDatabaseSettings.log_queries_filename,
                logFilename.getPath() ).newGraphDatabase();
        List<String> strings;

        try
        {
            database.execute( QUERY ).close();

            // File will not be created until query logging is enabled.
            assertFalse( fileSystem.fileExists( logFilename ) );

            database.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'true')" ).close();
            database.execute( QUERY ).close();

            // Both config change and query should exist
            strings = readAllLines( logFilename );
            assertEquals( 2, strings.size() );

            database.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'false')" ).close();
            database.execute( QUERY ).close();
        }
        finally
        {
            database.shutdown();
        }

        // Value should not change when disabled
        strings = readAllLines( logFilename );
        assertEquals( 2, strings.size() );
    }

    @Test
    public void logQueriesWithSystemTimeZoneIsConfigured()
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
        database = databaseBuilder.setConfig( log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM.name() )
                .setConfig( logs_directory, logsDirectory.getPath() )
                .newGraphDatabase();
        database.execute( QUERY ).close();
        database.shutdown();
    }

    private static void executeQueryAndShutdown( GraphDatabaseService database )
    {
        executeQueryAndShutdown( database, QUERY, Collections.emptyMap() );
    }

    private static void executeQueryAndShutdown( GraphDatabaseService database, String query, Map<String,Object> params )
    {
        Result execute = database.execute( query, params );
        execute.close();
        database.shutdown();
    }

    private static String clientConnectionInfo()
    {
        return ClientConnectionInfo.EMBEDDED_CONNECTION.withUsername( AUTH_DISABLED.username() ).asConnectionDetails();
    }

    private List<String> readAllLinesSilent( File logFilename )
    {
        try
        {
            return readAllLines( fileSystem.get(), logFilename );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private List<String> readAllLines( File logFilename ) throws IOException
    {
        return readAllLines( fileSystem.get(), logFilename );
    }

    static List<String> readAllLines( FileSystemAbstraction fs, File logFilename ) throws IOException
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

    private static final int NOT_FOUND = -1;

    private static int findLogLineThatContains( String expectedMessage, List<String> logLines )
    {
        int i = 0;
        for ( String line : logLines )
        {
            if ( line.contains( expectedMessage ) )
            {
                return i;
            }
            i++;
        }
        return NOT_FOUND;
    }
}
