/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorCounters;
import org.neo4j.kernel.api.query.CompilerInfo;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.FakeCpuClock;
import org.neo4j.test.FakeHeapAllocation;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static io.netty.channel.local.LocalAddress.ANY;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class ConfiguredQueryLoggerTest
{
    private static final String LOG_MESSAGE_TEMPLATE = "%d ms: %s - %s - {}";
    private static final ClientConnectionInfo SESSION_1 = new BoltConnectionInfo( "bolt-1", "client", ANY, ANY );
    private static final ClientConnectionInfo SESSION_2 = new BoltConnectionInfo( "bolt-2", "client", ANY, ANY );
    private static final ClientConnectionInfo SESSION_3 = new BoltConnectionInfo( "bolt-3", "client", ANY, ANY );
    private static final String QUERY_1 = "MATCH (n) RETURN n";
    private static final String QUERY_2 = "MATCH (a)--(b) RETURN b.name";
    private static final String QUERY_3 = "MATCH (c)-[:FOO]->(d) RETURN d.size";
    private static final String QUERY_4 = "MATCH (n) WHERE n.age IN $ages RETURN n";
    private final FakeClock clock = Clocks.fakeClock();
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final FakeCpuClock cpuClock = new FakeCpuClock();
    private final FakeHeapAllocation heapAllocation = new FakeHeapAllocation();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private long pageHits;
    private long pageFaults;
    private long thresholdInMillis = 10;
    private int queryId;

    @Test
    void shouldLogQuerySlowerThanThreshold()
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        logProvider.assertExactly(
            inLog( getClass() ).info( format( LOG_MESSAGE_TEMPLATE, 11L, expectedSessionString, QUERY_1 ) )
        );
    }

    @Test
    void shouldRespectThreshold()
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        logProvider.assertNoLoggingOccurred();

        // and when
        ExecutingQuery query2 = query( SESSION_2, "TestUser2", QUERY_2 );
        thresholdInMillis = 5;
        queryLogger = queryLogger( logProvider ); // Rebuild queryLogger, like the DynamicQueryLogger would.
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.success( query2 );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_2, "TestUser2" );
        logProvider.assertExactly(
                inLog( getClass() ).info( format( LOG_MESSAGE_TEMPLATE, 9L, expectedSessionString, QUERY_2 ) )
        );
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
        logProvider.assertExactly(
                inLog( getClass() ).info( format( LOG_MESSAGE_TEMPLATE, 17L, expectedSession2String, QUERY_2 ) ),
                inLog( getClass() ).info( format( LOG_MESSAGE_TEMPLATE, 25L, expectedSession1String, QUERY_1 ) )
        );
    }

    @Test
    void shouldLogQueryOnFailureEvenIfFasterThanThreshold()
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );
        RuntimeException failure = new RuntimeException();

        // when
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.failure( query, failure );

        // then
        logProvider.assertExactly(
                inLog( getClass() )
                        .error( is( "1 ms: " + sessionConnectionDetails( SESSION_1, "TestUser" )
                                + " - MATCH (n) RETURN n - {}" ), sameInstance( failure ) )
        );
    }

    @Test
    void shouldLogQueryParameters()
    {
        // given
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, databaseIdRepository.defaultDatabase(), "TestUser", QUERY_4, params, emptyMap() );
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( GraphDatabaseSettings.log_queries_parameter_logging_enabled, true ) );

        // when
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        logProvider.assertExactly(
            inLog( getClass() ).info( format( "%d ms: %s - %s - %s - {}", 11L, expectedSessionString, QUERY_4,
                    "{ages: " +
                    "[41, 42, 43]}" ) )
        );
    }

    @Test
    void shouldLogQueryParametersOnFailure()
    {
        // given
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, databaseIdRepository.defaultDatabase(), "TestUser", QUERY_4, params, emptyMap() );
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( GraphDatabaseSettings.log_queries_parameter_logging_enabled, true ) );
        RuntimeException failure = new RuntimeException();

        // when
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.failure( query, failure );

        // then
        logProvider.assertExactly(
            inLog( getClass() ).error(
                    is( "1 ms: " + sessionConnectionDetails( SESSION_1, "TestUser" )
                            + " - MATCH (n) WHERE n.age IN $ages RETURN n - {ages: [41, 42, 43]} - {}" ),
                sameInstance( failure ) )
        );
    }

    @Test
    void shouldLogUserName()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        ExecutingQuery anotherQuery = query( SESSION_1, "AnotherUser", QUERY_1 );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( anotherQuery );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( format( LOG_MESSAGE_TEMPLATE, 10L,
                        sessionConnectionDetails( SESSION_1, "TestUser" ), QUERY_1 ) ),
                inLog( getClass() ).info( format( LOG_MESSAGE_TEMPLATE, 10L,
                        sessionConnectionDetails( SESSION_1, "AnotherUser" ), QUERY_1 ) )
        );
    }

    @Test
    void shouldLogMetaData()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        ExecutingQuery query = query( SESSION_1, databaseIdRepository.defaultDatabase(), "TestUser", QUERY_1, emptyMap(), map( "User", "UltiMate" ) );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        ExecutingQuery anotherQuery =
                query( SESSION_1, databaseIdRepository.defaultDatabase(), "AnotherUser", QUERY_1, emptyMap(), map( "Place", "Town" ) );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        Throwable error = new Throwable();
        queryLogger.failure( anotherQuery, error );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - {User: 'UltiMate'}", 10L,
                        sessionConnectionDetails( SESSION_1, "TestUser" ), QUERY_1
                ) ),
                inLog( getClass() ).error(
                        equalTo( format( "%d ms: %s - %s - {Place: 'Town'}", 10L,
                            sessionConnectionDetails( SESSION_1, "AnotherUser" ), QUERY_1 ) ),
                        sameInstance( error ) )
        );
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
    void shouldNotLogPasswordWhenMalformedArgument()
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('user, 'password')";
        String outputQuery = "CALL dbms.security.changeUserPassword('user, '******')";

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
                Config.defaults( GraphDatabaseSettings.log_queries_parameter_logging_enabled, true ) );

        // when
        ExecutingQuery query = query( SESSION_1, databaseIdRepository.defaultDatabase(), "neo", inputQuery, params, emptyMap() );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        logProvider.assertExactly( inLog( getClass() )
                .info( format( "%d ms: %s - %s - {%s} - {}", 10L, sessionConnectionDetails( SESSION_1, "neo" ),
                        outputQuery,
                        paramsString ) ) );
    }

    @Test
    void shouldBeAbleToLogDetailedTime()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        clock.forward( 17, TimeUnit.MILLISECONDS );
        cpuClock.add( 12, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        logProvider.assertExactly( inLog( getClass() ).info(
                containsString( "17 ms: (planning: 17, cpu: 12, waiting: 0) - " ) ) );
    }

    @Test
    void shouldBeAbleToLogAllocatedBytes()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( GraphDatabaseSettings.log_queries_allocation_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        clock.forward( 17, TimeUnit.MILLISECONDS );
        heapAllocation.add( 4096 );
        queryLogger.success( query );

        // then
        logProvider.assertExactly( inLog( getClass() ).info(
                containsString( "ms: 4096 B - " ) ) );
    }

    @Test
    void shouldBeAbleToLogPageHitsAndPageFaults()
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider,
                Config.defaults( GraphDatabaseSettings.log_queries_page_detail_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        clock.forward( 12, TimeUnit.MILLISECONDS );
        pageHits = 17;
        pageFaults = 12;
        queryLogger.success( query );

        // then
        logProvider.assertExactly( inLog( getClass() ).info(
                containsString( " 17 page hits, 12 page faults - " ) ) );
    }

    @Test
    void shouldLogRuntime()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, databaseIdRepository.defaultDatabase(), "TestUser", QUERY_4, params, emptyMap() );
        Config config = Config.defaults();
        config.set( GraphDatabaseSettings.log_queries_parameter_logging_enabled, true );
        config.set( GraphDatabaseSettings.log_queries_runtime_logging_enabled, true );
        QueryLogger queryLogger = queryLogger( logProvider, config );

        // when
        clock.forward( 11, TimeUnit.MILLISECONDS );
        query.compilationCompleted( new CompilerInfo( "magic", "quantum", Collections.emptyList() ), null );
        queryLogger.success( query );

        // then
        String expectedSessionString = sessionConnectionDetails( SESSION_1, "TestUser" );
        logProvider.assertExactly(
                inLog( getClass() ).info( format( "%d ms: %s - %s - %s - {}", 11L, expectedSessionString, QUERY_4,
                        "{ages: [41, 42, 43]} - runtime=quantum" ) )
        );
    }

    @Test
    void includeDatabaseNameIntoQueryLogString()
    {
        ConfiguredQueryLogger queryLogger = queryLogger( logProvider );

        // when
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        ExecutingQuery anotherQuery = query( SESSION_1, databaseIdRepository.get( "otherDb" ), "AnotherUser", QUERY_1 );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( anotherQuery );

        // then
        logProvider.assertExactly(
                inLog( getClass() ).info( format( LOG_MESSAGE_TEMPLATE, 10L,
                        sessionConnectionDetails( SESSION_1, DEFAULT_DATABASE_NAME, "TestUser" ), QUERY_1 ) ),
                inLog( getClass() ).info( format( LOG_MESSAGE_TEMPLATE, 10L,
                        sessionConnectionDetails( SESSION_1, "otherdb", "AnotherUser" ), QUERY_1 ) )
        );
    }

    private ConfiguredQueryLogger queryLogger( LogProvider logProvider )
    {
        return queryLogger( logProvider, Config.defaults( GraphDatabaseSettings.log_queries_parameter_logging_enabled, false ) );
    }

    private ConfiguredQueryLogger queryLogger( LogProvider logProvider, Config config )
    {
        config.set( GraphDatabaseSettings.log_queries_threshold, Duration.ofMillis( thresholdInMillis ) );
        return new ConfiguredQueryLogger( logProvider.getLog( getClass() ), config );
    }

    private ExecutingQuery query(
            ClientConnectionInfo sessionInfo,
            String username,
            String queryText )
    {
        return query( sessionInfo, databaseIdRepository.defaultDatabase(), username, queryText );
    }

    private ExecutingQuery query(
            ClientConnectionInfo sessionInfo,
            DatabaseId databaseId,
            String username,
            String queryText )
    {
        return query( sessionInfo, databaseId, username, queryText, emptyMap(), emptyMap() );
    }

    private String sessionConnectionDetails( ClientConnectionInfo sessionInfo, String username )
    {
        return sessionConnectionDetails( sessionInfo,  DEFAULT_DATABASE_NAME, username );
    }

    private String sessionConnectionDetails( ClientConnectionInfo sessionInfo, String databaseName, String username )
    {
        return sessionInfo.asConnectionDetails()  + "\t" + databaseName + " - " + username ;
    }

    private ExecutingQuery query(
            ClientConnectionInfo sessionInfo,
            DatabaseId databaseId,
            String username,
            String queryText,
            Map<String,Object> params,
            Map<String,Object> metaData )
    {
        Thread thread = Thread.currentThread();
        return new ExecutingQuery( queryId++,
                sessionInfo, databaseId, username, queryText,
                ValueUtils.asMapValue( params ),
                metaData,
                () -> 0,
                new PageCursorCounters()
                {
                    @Override
                    public long faults()
                    {
                        return pageFaults;
                    }

                    @Override
                    public long hits()
                    {
                        return pageHits;
                    }

                    @Override
                    public long pins()
                    {
                        return 0;
                    }

                    @Override
                    public long unpins()
                    {
                        return 0;
                    }

                    @Override
                    public long bytesRead()
                    {
                        return 0;
                    }

                    @Override
                    public long evictions()
                    {
                        return 0;
                    }

                    @Override
                    public long evictionExceptions()
                    {
                        return 0;
                    }

                    @Override
                    public long bytesWritten()
                    {
                        return 0;
                    }

                    @Override
                    public long flushes()
                    {
                        return 0;
                    }

                    @Override
                    public double hitRatio()
                    {
                        return 0d;
                    }
                },
                thread.getId(),
                thread.getName(),
                clock,
                cpuClock,
                heapAllocation );
    }
}
