/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.auth.UserManagementProcedures;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.CypherQueryObfuscatorFactory;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.query.CompilerInfo;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QueryObfuscator;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.FakeCpuClock;
import org.neo4j.test.FakeMemoryTracker;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static io.netty.channel.local.LocalAddress.ANY;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel.VERBOSE;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_parameter_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_runtime_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_threshold;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

class ConfiguredQueryLoggerTest
{
    private static final String LOG_MESSAGE_TEMPLATE = "%d ms: %s - %s - {}";
    private static final String VERBOSE_LOG_MESSAGE_TEMPLATE = "id:%d - " + LOG_MESSAGE_TEMPLATE;
    private static final String VERBOSE_START_LOG_MESSAGE_TEMPLATE = "Query started: " + VERBOSE_LOG_MESSAGE_TEMPLATE;
    private static final ClientConnectionInfo SESSION_1 = new BoltConnectionInfo( "bolt-1", "client", ANY, ANY );
    private static final ClientConnectionInfo SESSION_2 = new BoltConnectionInfo( "bolt-2", "client", ANY, ANY );
    private static final ClientConnectionInfo SESSION_3 = new BoltConnectionInfo( "bolt-3", "client", ANY, ANY );
    private static final String QUERY_1 = "MATCH (n) RETURN n";
    private static final String QUERY_2 = "MATCH (a)--(b) RETURN b.name";
    private static final String QUERY_3 = "MATCH (c)-[:FOO]->(d) RETURN d.size";
    private static final String QUERY_4 = "MATCH (n) WHERE n.age IN $ages RETURN n";
    private final FakeClock clock = Clocks.fakeClock();
    private final NamedDatabaseId defaultDbId = randomNamedDatabaseId();
    private final FakeCpuClock cpuClock = new FakeCpuClock();
    private final FakeMemoryTracker memoryTracker = new FakeMemoryTracker();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private long pageHits;
    private long pageFaults;
    private long thresholdInMillis = 10;
    private int queryId;
    private final ObfuscatorFactory obfuscatorFactory = new ObfuscatorFactory();

    @Test
    void shouldLogQuerySlowerThanThreshold()
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( format( LOG_MESSAGE_TEMPLATE, 11L, expectedSessionString, QUERY_1 ) );
    }

    @Test
    void shouldRespectThreshold()
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // and when
        ExecutingQuery query2 = query( SESSION_2, "TestUser2", QUERY_2 );
        thresholdInMillis = 5;
        queryLogger = queryLogger( logProvider ); // Rebuild queryLogger, like the DynamicQueryLogger would.
        clock.forward( 9, TimeUnit.MILLISECONDS );
        query2.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        queryLogger.success( query2 );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_2, "TestUser2" );
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO )
                .containsMessages( format( LOG_MESSAGE_TEMPLATE, 9L, expectedSessionString, QUERY_2 ) );
    }

    @Test
    void shouldKeepTrackOfDifferentSessions()
    {
        // given
        ExecutingQuery query1 = query( SESSION_1, "TestUser1", QUERY_1 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        ExecutingQuery query2 = query( SESSION_2, "TestUser2", QUERY_2 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        ExecutingQuery query3 = query( SESSION_3, "TestUser3", QUERY_3 );

        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        query1.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        query2.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        query3.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );

        // when
        clock.forward( 1, TimeUnit.MILLISECONDS );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.success( query3 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.success( query2 );
        clock.forward( 7, TimeUnit.MILLISECONDS );
        queryLogger.success( query1 );

        // then
        String expectedSession1String = sessionConnectionDetails( SESSION_1, "TestUser1" );
        String expectedSession2String = sessionConnectionDetails( SESSION_2, "TestUser2" );
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO )
                .containsMessages( format( LOG_MESSAGE_TEMPLATE, 17L, expectedSession2String, QUERY_2 ),
                                   format( LOG_MESSAGE_TEMPLATE, 25L, expectedSession1String, QUERY_1 ) );
    }

    @Test
    void shouldLogQueryOnFailureEvenIfFasterThanThreshold()
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );
        RuntimeException failure = new RuntimeException();

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.failure( query, failure );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR ).containsMessageWithException(
                "1 ms: " + sessionConnectionDetails( SESSION_1, "TestUser" ) + " - MATCH (n) RETURN n - {}", failure );
    }

    @Test
    void shouldLogQueryParameters()
    {
        // given
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, defaultDbId, "TestUser", QUERY_4, params, emptyMap() );
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( log_queries_parameter_logging_enabled, true ) );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO )
                .containsMessages( format( "%d ms: %s - %s - %s - {}", 11L, expectedSessionString, QUERY_4,
                    "{ages: " +
                    "[41, 42, 43]}" ) );
    }

    @Test
    void shouldLogQueryParametersOnFailure()
    {
        // given
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, defaultDbId, "TestUser", QUERY_4, params, emptyMap() );
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( log_queries_parameter_logging_enabled, true ) );
        RuntimeException failure = new RuntimeException();

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.failure( query, failure );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR )
                .containsMessageWithException( "1 ms: " + sessionConnectionDetails( SESSION_1, "TestUser" ) +
                                " - MATCH (n) WHERE n.age IN $ages RETURN n - {ages: [41, 42, 43]} - {}", failure );
    }

    @Test
    void shouldLogUserName()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        ExecutingQuery anotherQuery = query( SESSION_1, "AnotherUser", QUERY_1 );
        anotherQuery.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( anotherQuery );

        // then
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO )
                .containsMessages( format( LOG_MESSAGE_TEMPLATE, 10L,
                        sessionConnectionDetails( SESSION_1, "TestUser" ), QUERY_1 ),
                        format( LOG_MESSAGE_TEMPLATE, 10L,
                        sessionConnectionDetails( SESSION_1, "AnotherUser" ), QUERY_1 ) );
    }

    @Test
    void shouldLogMetaData()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        ExecutingQuery query = query( SESSION_1, defaultDbId, "TestUser", QUERY_1, emptyMap(), map( "User", "UltiMate" ) );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        ExecutingQuery anotherQuery =
                query( SESSION_1, defaultDbId, "AnotherUser", QUERY_1, emptyMap(), map( "Place", "Town" ) );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        Throwable error = new Throwable();
        anotherQuery.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        queryLogger.failure( anotherQuery, error );

        // then
        assertThat( logProvider ).forClass( getClass() )
                .forLevel( INFO )
                .containsMessages( format( "%d ms: %s - %s - {User: 'UltiMate'}", 10L,
                        sessionConnectionDetails( SESSION_1, "TestUser" ), QUERY_1 ) )
                .forLevel( ERROR )
                .containsMessageWithException( format( "%d ms: %s - %s - {Place: 'Town'}", 10L,
                            sessionConnectionDetails( SESSION_1, "AnotherUser" ), QUERY_1 ), error );
    }

    @Test
    void shouldNotLogPassword()
    {
        String inputQuery = "CALL dbms.security.changePassword('abc123')";
        String outputQuery = "CALL dbms.security.changePassword('******')";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogChangeUserPassword()
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('user', 'abc123')";
        String outputQuery = "CALL dbms.security.changeUserPassword('user', '******')";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogChangeUserPasswordWithChangeRequired()
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('user', 'abc123', true)";
        String outputQuery = "CALL dbms.security.changeUserPassword('user', '******', true)";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogPasswordNull()
    {
        String inputQuery = "CALL dbms.security.changeUserPassword(null, 'password')";
        String outputQuery = "CALL dbms.security.changeUserPassword(null, '******')";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogPasswordExplain()
    {
        String inputQuery = "EXPLAIN CALL dbms.security.changePassword('abc123')";
        String outputQuery = "EXPLAIN CALL dbms.security.changePassword('******')";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogPasswordEvenIfPasswordIsSilly()
    {
        String inputQuery = "CALL dbms.security.changePassword('.changePassword(\\'si\"lly\\')')";
        String outputQuery = "CALL dbms.security.changePassword('******')";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogPasswordEvenIfYouDoTwoThingsAtTheSameTime()
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('neo4j','.changePassword(silly)') " +
                            "CALL dbms.security.changeUserPassword('smith','other$silly') RETURN 1";
        String outputQuery = "CALL dbms.security.changeUserPassword('neo4j','******') " +
                             "CALL dbms.security.changeUserPassword('smith','******') RETURN 1";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogCreateUserPassword()
    {
        String inputQuery = "CALL dbms.security.createUser('user', 'abc123')";
        String outputQuery = "CALL dbms.security.createUser('user', '******')";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogCreateUserPasswordWithRequiredChange()
    {
        String inputQuery = "CALL dbms.security.createUser('user', 'abc123', true)";
        String outputQuery = "CALL dbms.security.createUser('user', '******', true)";

        runAndCheck( inputQuery, outputQuery, emptyMap(), "" );
    }

    @Test
    void shouldNotLogPasswordEvenIfYouDoTwoThingsAtTheSameTimeWithSeveralParms()
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('neo4j',$first) " +
                            "CALL dbms.security.changeUserPassword('smith',$second) RETURN 1";
        String outputQuery = "CALL dbms.security.changeUserPassword('neo4j',$first) " +
                             "CALL dbms.security.changeUserPassword('smith',$second) RETURN 1";

        Map<String,Object> params = new HashMap<>();
        params.put( "first", ".changePassword(silly)" );
        params.put( "second", ".other$silly" );

        runAndCheck( inputQuery, outputQuery, params, "first: '******', second: '******'" );
    }

    @Test
    void shouldNotLogPasswordInParams()
    {
        String inputQuery = "CALL dbms.security.changePassword($password)";
        String outputQuery = "CALL dbms.security.changePassword($password)";

        runAndCheck( inputQuery, outputQuery, Collections.singletonMap( "password", ".changePassword(silly)" ),
                     "password: '******'" );
    }

    @Test
    void shouldNotLogPasswordInDeprecatedParams()
    {
        String inputQuery = "CALL dbms.security.changePassword($password)";
        String outputQuery = "CALL dbms.security.changePassword($password)";

        runAndCheck( inputQuery, outputQuery, Collections.singletonMap( "password", "abc123" ), "password: '******'" );
    }

    @Test
    void shouldNotLogPasswordDifferentWhitespace()
    {
        String inputQuery = "CALL dbms.security.changeUserPassword(%s'abc123'%s)";
        String outputQuery = "CALL dbms.security.changeUserPassword(%s'******'%s)";

        runAndCheck(
                format( inputQuery, "'user',", "" ),
                format( outputQuery, "'user',", "" ), emptyMap(), "" );
        runAndCheck(
                format( inputQuery, "'user', ", "" ),
                format( outputQuery, "'user', ", "" ), emptyMap(), "" );
        runAndCheck(
                format( inputQuery, "'user' ,", " " ),
                format( outputQuery, "'user' ,", " " ), emptyMap(), "" );
        runAndCheck(
                format( inputQuery, "'user',  ", "  " ),
                format( outputQuery, "'user',  ", "  " ), emptyMap(), "" );
    }

    private void runAndCheck( String inputQuery, String outputQuery, Map<String,Object> params, String paramsString )
    {
        logProvider.clear();
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                                                         Config.defaults( log_queries_parameter_logging_enabled, true ) );

        QueryObfuscator ob = obfuscatorFactory.obfuscatorForQuery( inputQuery );

        // when
        ExecutingQuery query = query( SESSION_1, defaultDbId, "neo", inputQuery, params, emptyMap() );
        query.onObfuscatorReady( ob );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO )
                                 .containsMessages( format( "%d ms: %s - %s - {%s} - {}", 10L, sessionConnectionDetails( SESSION_1, "neo" ),
                                                            outputQuery,
                                                            paramsString ) );    }

    @Test
    void shouldBeAbleToLogDetailedTime()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 17, TimeUnit.MILLISECONDS );
        cpuClock.add( 12, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( INFO ).containsMessages( "17 ms: (planning: 17, cpu: 12, waiting: 0) - " );
    }

    @Test
    void shouldBeAbleToLogAllocatedBytes()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( GraphDatabaseSettings.log_queries_allocation_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        query.onCompilationCompleted( null, null, null );
        query.onExecutionStarted( memoryTracker );

        // when
        clock.forward( 17, TimeUnit.MILLISECONDS );
        memoryTracker.add( 4096 );
        queryLogger.success( query );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( INFO ).containsMessages( "ms: 4096 B - " );
    }

    @Test
    void shouldBeAbleToLogPageHitsAndPageFaults()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( GraphDatabaseSettings.log_queries_page_detail_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 12, TimeUnit.MILLISECONDS );
        pageHits = 17;
        pageFaults = 12;
        queryLogger.success( query );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( INFO ).containsMessages( " 17 page hits, 12 page faults - " );
    }

    @Test
    void shouldLogRuntime()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, defaultDbId, "TestUser", QUERY_4, params, emptyMap() );
        Config config = Config.defaults();
        config.set( log_queries_parameter_logging_enabled, true );
        config.set( GraphDatabaseSettings.log_queries_runtime_logging_enabled, true );
        QueryLogger queryLogger = queryLogger( logProvider, config );

        // when
        clock.forward( 11, TimeUnit.MILLISECONDS );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        query.onCompilationCompleted( new CompilerInfo( "magic", "quantum", Collections.emptyList() ), READ_ONLY, null );
        queryLogger.success( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO )
                .containsMessages( format( "%d ms: %s - %s - %s - {}", 11L, expectedSessionString, QUERY_4,
                        "{ages: [41, 42, 43]} - runtime=quantum" ) );
    }

    @Test
    void includeDatabaseNameIntoQueryLogString()
    {
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        var otherDbId = randomNamedDatabaseId();
        ExecutingQuery anotherQuery = query( SESSION_1, otherDbId, "AnotherUser", QUERY_1 );
        anotherQuery.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( anotherQuery );

        // then
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO ).containsMessages(
                format( LOG_MESSAGE_TEMPLATE, 10L,
                        sessionConnectionDetails( SESSION_1, defaultDbId.name(), "TestUser" ), QUERY_1 ),
                format( LOG_MESSAGE_TEMPLATE, 10L,
                        sessionConnectionDetails( SESSION_1, otherDbId.name(), "AnotherUser" ), QUERY_1 ) );
    }

    @Test
    void shouldIncludeQueryIdInVerboseLogging()
    {
        Config config = Config.newBuilder()
                .set( log_queries, VERBOSE )
                .set( log_queries_parameter_logging_enabled, false )
                .set( log_queries_threshold, Duration.ofMillis( thresholdInMillis ) )
                .set( log_queries_runtime_logging_enabled, false )
                .build();
        ConfiguredQueryLogger queryLogger = new ConfiguredQueryLogger( logProvider.getLog( getClass() ), config );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        queryLogger.start( query );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO )
                .containsMessages( format( VERBOSE_START_LOG_MESSAGE_TEMPLATE, query.internalQueryId(), 0L, expectedSessionString, QUERY_1 ),
                                   format( VERBOSE_LOG_MESSAGE_TEMPLATE, query.internalQueryId(), 11L, expectedSessionString, QUERY_1 ) );
    }

    @Test
    void shoulHaveDifferentIdsForDifferentQueries()
    {
        Config config = Config.newBuilder()
                .set( log_queries, VERBOSE )
                .set( log_queries_parameter_logging_enabled, false )
                .set( log_queries_threshold, Duration.ofMillis( thresholdInMillis ) )
                .set( log_queries_runtime_logging_enabled, false )
                .build();
        ConfiguredQueryLogger queryLogger = new ConfiguredQueryLogger( logProvider.getLog( getClass() ), config );
        ExecutingQuery query1 = query( SESSION_1, "TestUser", QUERY_1 );
        ExecutingQuery query2 = query( SESSION_1, "TestUser", QUERY_1 ); //same query text, distinguishable only by id

        query1.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        query2.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );

        // when
        queryLogger.start( query1 );
        clock.forward( 5, TimeUnit.MILLISECONDS );
        queryLogger.start( query2 );
        clock.forward( 5, TimeUnit.MILLISECONDS );
        queryLogger.success( query2 );
        clock.forward( 5, TimeUnit.MILLISECONDS );
        queryLogger.success( query1 );

        // then
        Assertions.assertNotEquals( query1.internalQueryId(), query2.internalQueryId(), "Queries should have different ids");
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        assertThat( logProvider ).forClass( this.getClass() ).forLevel( INFO )
                .containsMessages( format( VERBOSE_START_LOG_MESSAGE_TEMPLATE, query1.internalQueryId(), 0L, expectedSessionString, QUERY_1 ),
                format( VERBOSE_START_LOG_MESSAGE_TEMPLATE, query2.internalQueryId(), 5L, expectedSessionString, QUERY_1 ),
                format( VERBOSE_LOG_MESSAGE_TEMPLATE, query2.internalQueryId(), 10L, expectedSessionString, QUERY_1 ),
                format( VERBOSE_LOG_MESSAGE_TEMPLATE, query1.internalQueryId(), 15L, expectedSessionString, QUERY_1 ) );
    }

    private ConfiguredQueryLogger queryLogger( LogProvider logProvider )
    {
        return queryLogger( logProvider, Config.defaults( log_queries_parameter_logging_enabled, false ) );
    }

    private ConfiguredQueryLogger queryLogger( LogProvider logProvider, Config config )
    {
        config.set( log_queries_threshold, Duration.ofMillis( thresholdInMillis ) );
        config.set( log_queries, GraphDatabaseSettings.LogQueryLevel.INFO );
        if ( !config.isExplicitlySet( log_queries_runtime_logging_enabled ) )
        {
            config.set( log_queries_runtime_logging_enabled, false );
        }
        return new ConfiguredQueryLogger( logProvider.getLog( getClass() ), config );
    }

    private ExecutingQuery query(
            ClientConnectionInfo sessionInfo,
            String username,
            String queryText )
    {
        return query( sessionInfo, defaultDbId, username, queryText );
    }

    private ExecutingQuery query(
            ClientConnectionInfo sessionInfo,
            NamedDatabaseId namedDatabaseId,
            String username,
            String queryText )
    {
        return query( sessionInfo, namedDatabaseId, username, queryText, emptyMap(), emptyMap() );
    }

    private String sessionConnectionDetails( ClientConnectionInfo sessionInfo, String username )
    {
        return sessionConnectionDetails( sessionInfo, defaultDbId.name(), username );
    }

    private String sessionConnectionDetails( ClientConnectionInfo sessionInfo, String databaseName, String username )
    {
        return sessionInfo.asConnectionDetails()  + "\t" + databaseName + " - " + username ;
    }

    private ExecutingQuery query(
            ClientConnectionInfo sessionInfo,
            NamedDatabaseId namedDatabaseId,
            String username,
            String queryText,
            Map<String,Object> params,
            Map<String,Object> metaData )
    {
        Thread thread = Thread.currentThread();
        return new ExecutingQuery( queryId++,
                sessionInfo, namedDatabaseId, username, queryText,
                ValueUtils.asMapValue( params ),
                metaData,
                () -> 0, () -> pageHits, () -> pageFaults,
                thread.getId(),
                thread.getName(),
                clock,
                cpuClock );
    }

    private static class ObfuscatorFactory extends CypherQueryObfuscatorFactory
    {
        ObfuscatorFactory()
        {
            // required by procedure compiler
            registerComponent( EnterpriseSecurityContext.class );
            registerComponent( Transaction.class );
            registerComponent( GraphDatabaseAPI.class ) ;
            registerComponent( SecurityLog.class );

            registerProcedure( UserManagementProcedures.class );
        }
    }
}
