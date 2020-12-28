/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.auth.UserManagementProcedures;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.CypherQueryObfuscatorFactory;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.query.CompilerInfo;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QueryObfuscator;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.logging.Level;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.LogExtended;
import org.neo4j.test.FakeCpuClock;
import org.neo4j.test.FakeMemoryTracker;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static io.netty.channel.local.LocalAddress.ANY;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel.VERBOSE;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_allocation_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_parameter_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_runtime_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_threshold;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.logging.LogAssertions.assertThat;

class ConfiguredQueryLoggerTest
{
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
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private long pageHits;
    private long pageFaults;
    private long thresholdInMillis = 10;
    private int queryId;
    private final ObfuscatorFactory obfuscatorFactory = new ObfuscatorFactory();

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldLogQuerySlowerThanThreshold( FormattedLogFormat logFormat )
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 11 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_1 ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldRespectThreshold( FormattedLogFormat logFormat )
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        queryLogger.start( query );
        clock.forward( 9, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertThat( outContent.size() ).isEqualTo( 0 );

        // and when
        ExecutingQuery query2 = query( SESSION_2, "TestUser2", QUERY_2 );
        thresholdInMillis = 5;
        queryLogger = queryLogger( logFormat ); // Rebuild queryLogger, like the DynamicQueryLogger would.
        clock.forward( 9, TimeUnit.MILLISECONDS );
        query2.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        queryLogger.success( query2 );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 9 )
                .source( SESSION_2.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser2" )
                .query( QUERY_2 ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldKeepTrackOfDifferentSessions( FormattedLogFormat logFormat )
    {
        // given
        ExecutingQuery query1 = query( SESSION_1, "TestUser1", QUERY_1 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        ExecutingQuery query2 = query( SESSION_2, "TestUser2", QUERY_2 );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        ExecutingQuery query3 = query( SESSION_3, "TestUser3", QUERY_3 );

        ConfiguredQueryLogger queryLogger = queryLogger( logFormat );

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
        assertLog( logFormat, outContent.toString() ).contains(
                line()
                        .level( Level.INFO )
                        .elapsed( 17 )
                        .source( SESSION_2.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser2" )
                        .query( QUERY_2 ),
                line()
                        .level( Level.INFO )
                        .elapsed( 25 )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser1" )
                        .query( QUERY_1 ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldLogQueryOnFailureEvenIfFasterThanThreshold( FormattedLogFormat logFormat )
    {
        // given
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        ConfiguredQueryLogger queryLogger = queryLogger( logFormat );
        RuntimeException failure = new RuntimeException( "test exception" );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.failure( query, failure );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.ERROR )
                .elapsed( 1 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_1 ), failure );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldLogQueryParameters( FormattedLogFormat logFormat )
    {
        // given
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, defaultDbId, "TestUser", QUERY_4, params, emptyMap() );
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, Config.defaults( log_queries_parameter_logging_enabled, true ) );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 11 )
                .allocatedBytes( 0 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_4 )
                .params( "{ages: [41, 42, 43]}" ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldLogQueryParametersOnFailure( FormattedLogFormat logFormat )
    {
        // given
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, defaultDbId, "TestUser", QUERY_4, params, emptyMap() );
        Config config = Config.newBuilder()
                .set( log_queries_parameter_logging_enabled, true )
                .set( log_queries_allocation_logging_enabled, false )
                .build();
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, config );
        RuntimeException failure = new RuntimeException( "test exception" );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        queryLogger.failure( query, failure );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.ERROR )
                .elapsed( 1 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_4 )
                .params( "{ages: [41, 42, 43]}" ), failure );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldLogUserName( FormattedLogFormat logFormat )
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat );

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
        assertLog( logFormat, outContent.toString() ).contains(
                line()
                        .level( Level.INFO )
                        .elapsed( 10 )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ),
                line()
                        .level( Level.INFO )
                        .elapsed( 10 )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "AnotherUser" )
                        .query( QUERY_1 ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldLogMetaData( FormattedLogFormat logFormat )
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat );

        // when
        ExecutingQuery query = query( SESSION_1, defaultDbId, "TestUser", QUERY_1, emptyMap(), map( "User", "UltiMate" ) );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 10 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_1 )
                .additional( "{User: 'UltiMate'}" ) );
        outContent.reset();

        // and when
        ExecutingQuery anotherQuery =
                query( SESSION_1, defaultDbId, "AnotherUser", QUERY_1, emptyMap(), map( "Place", "Town" ) );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        Throwable error = new Throwable( "test exception" );
        anotherQuery.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        queryLogger.failure( anotherQuery, error );

        // and then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.ERROR )
                .elapsed( 10 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "AnotherUser" )
                .query( QUERY_1 )
                .additional( "{Place: 'Town'}" ), error );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPassword( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changePassword('abc123')";
        String outputQuery = "CALL dbms.security.changePassword('******')";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogChangeUserPassword( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('user', 'abc123')";
        String outputQuery = "CALL dbms.security.changeUserPassword('user', '******')";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogChangeUserPasswordWithChangeRequired( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('user', 'abc123', true)";
        String outputQuery = "CALL dbms.security.changeUserPassword('user', '******', true)";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPasswordNull( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changeUserPassword(null, 'password')";
        String outputQuery = "CALL dbms.security.changeUserPassword(null, '******')";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPasswordExplain( FormattedLogFormat logFormat )
    {
        String inputQuery = "EXPLAIN CALL dbms.security.changePassword('abc123')";
        String outputQuery = "EXPLAIN CALL dbms.security.changePassword('******')";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPasswordEvenIfPasswordIsSilly( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changePassword('.changePassword(\\'si\"lly\\')')";
        String outputQuery = "CALL dbms.security.changePassword('******')";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPasswordEvenIfYouDoTwoThingsAtTheSameTime( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('neo4j','.changePassword(silly)') " +
                            "CALL dbms.security.changeUserPassword('smith','other$silly') RETURN 1";
        String outputQuery = "CALL dbms.security.changeUserPassword('neo4j','******') " +
                             "CALL dbms.security.changeUserPassword('smith','******') RETURN 1";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogCreateUserPassword( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.createUser('user', 'abc123')";
        String outputQuery = "CALL dbms.security.createUser('user', '******')";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogCreateUserPasswordWithRequiredChange( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.createUser('user', 'abc123', true)";
        String outputQuery = "CALL dbms.security.createUser('user', '******', true)";

        runAndCheck( logFormat, inputQuery, outputQuery, emptyMap(), "{}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPasswordEvenIfYouDoTwoThingsAtTheSameTimeWithSeveralParms( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changeUserPassword('neo4j',$first) " +
                            "CALL dbms.security.changeUserPassword('smith',$second) RETURN 1";
        String outputQuery = "CALL dbms.security.changeUserPassword('neo4j',$first) " +
                             "CALL dbms.security.changeUserPassword('smith',$second) RETURN 1";

        Map<String,Object> params = new HashMap<>();
        params.put( "first", ".changePassword(silly)" );
        params.put( "second", ".other$silly" );

        runAndCheck( logFormat, inputQuery, outputQuery, params, "{first: '******', second: '******'}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPasswordInParams( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changePassword($password)";
        String outputQuery = "CALL dbms.security.changePassword($password)";

        runAndCheck( logFormat, inputQuery, outputQuery, Collections.singletonMap( "password", ".changePassword(silly)" ),
                     "{password: '******'}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPasswordInDeprecatedParams( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changePassword($password)";
        String outputQuery = "CALL dbms.security.changePassword($password)";

        runAndCheck( logFormat, inputQuery, outputQuery, Collections.singletonMap( "password", "abc123" ), "{password: '******'}" );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldNotLogPasswordDifferentWhitespace( FormattedLogFormat logFormat )
    {
        String inputQuery = "CALL dbms.security.changeUserPassword(%s'abc123'%s)";
        String outputQuery = "CALL dbms.security.changeUserPassword(%s'******'%s)";

        runAndCheck(
                logFormat, format( inputQuery, "'user',", "" ),
                format( outputQuery, "'user',", "" ), emptyMap(), "{}" );
        runAndCheck(
                logFormat, format( inputQuery, "'user', ", "" ),
                format( outputQuery, "'user', ", "" ), emptyMap(), "{}" );
        runAndCheck(
                logFormat, format( inputQuery, "'user' ,", " " ),
                format( outputQuery, "'user' ,", " " ), emptyMap(), "{}" );
        runAndCheck(
                logFormat, format( inputQuery, "'user',  ", "  " ),
                format( outputQuery, "'user',  ", "  " ), emptyMap(), "{}" );
    }

    private void runAndCheck( FormattedLogFormat logFormat, String inputQuery, String outputQuery, Map<String,Object> params,
            String paramsString )
    {
        outContent.reset();
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, Config.defaults( log_queries_parameter_logging_enabled, true ) );

        QueryObfuscator ob = obfuscatorFactory.obfuscatorForQuery( inputQuery );

        // when
        ExecutingQuery query = query( SESSION_1, defaultDbId, "neo", inputQuery, params, emptyMap() );
        query.onObfuscatorReady( ob );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 10 )
                .allocatedBytes( 0 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "neo" )
                .query( outputQuery )
                .params( paramsString ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldBeAbleToLogDetailedTime( FormattedLogFormat logFormat )
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, Config.defaults( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 17, TimeUnit.MILLISECONDS );
        cpuClock.add( 12, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 17 )
                .planning( 17 )
                .cpu( 12 )
                .waiting( 0 )
                .allocatedBytes( 0 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_1 )
                .params( "{}" ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldBeAbleToLogAllocatedBytes( FormattedLogFormat logFormat )
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, Config.defaults( log_queries_allocation_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        query.onCompilationCompleted( null, null, null );
        query.onExecutionStarted( memoryTracker );

        // when
        clock.forward( 17, TimeUnit.MILLISECONDS );
        memoryTracker.add( 4096 );
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 17 )
                .allocatedBytes( 4096 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_1 )
                .params( "{}" ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldBeAbleToLogPageHitsAndPageFaults( FormattedLogFormat logFormat )
    {
        // given
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, Config.defaults( GraphDatabaseSettings.log_queries_page_detail_logging_enabled, true ) );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        clock.forward( 12, TimeUnit.MILLISECONDS );
        pageHits = 17;
        pageFaults = 12;
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 12 )
                .allocatedBytes( 0 )
                .pageHits( 17 )
                .pageFaults( 12 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_1 )
                .params( "{}" ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldLogRuntime( FormattedLogFormat logFormat )
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "ages", Arrays.asList( 41, 42, 43 ) );
        ExecutingQuery query = query( SESSION_1, defaultDbId, "TestUser", QUERY_4, params, emptyMap() );
        Config config = Config.defaults();
        config.set( log_queries_parameter_logging_enabled, true );
        config.set( GraphDatabaseSettings.log_queries_runtime_logging_enabled, true );
        config.set( log_queries_allocation_logging_enabled, false );
        QueryLogger queryLogger = queryLogger( logFormat, config );

        // when
        clock.forward( 11, TimeUnit.MILLISECONDS );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        query.onCompilationCompleted( new CompilerInfo( "magic", "quantum", Collections.emptyList() ), READ_ONLY, null );
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains( line()
                .level( Level.INFO )
                .elapsed( 11 )
                .source( SESSION_1.asConnectionDetails() )
                .database( defaultDbId.name() )
                .user( "TestUser" )
                .query( QUERY_4 )
                .params( "{ages: [41, 42, 43]}" )
                .runtime( "quantum" ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void includeDatabaseNameIntoQueryLogString( FormattedLogFormat logFormat )
    {
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat );

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
        assertLog( logFormat, outContent.toString() ).contains(
                line()
                        .level( Level.INFO )
                        .elapsed( 10 )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ),
                line()
                        .level( Level.INFO )
                        .elapsed( 10 )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( otherDbId.name() )
                        .user( "AnotherUser" )
                        .query( QUERY_1 ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldIncludeQueryIdInVerboseLogging( FormattedLogFormat logFormat )
    {
        Config config = Config.newBuilder()
                .set( log_queries, VERBOSE )
                .set( log_queries_parameter_logging_enabled, false )
                .set( log_queries_runtime_logging_enabled, false )
                .set( log_queries_allocation_logging_enabled, false )
                .build();
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, config );
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );

        // when
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        queryLogger.start( query );
        clock.forward( 11, TimeUnit.MILLISECONDS );
        queryLogger.success( query );

        // then
        assertLog( logFormat, outContent.toString() ).contains(
                line()
                        .level( Level.INFO )
                        .started()
                        .elapsed( 0 )
                        .queryId( query.internalQueryId() )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ),
                line()
                        .level( Level.INFO )
                        .elapsed( 11 )
                        .queryId( query.internalQueryId() )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void shouldHaveDifferentIdsForDifferentQueries( FormattedLogFormat logFormat )
    {
        Config config = Config.newBuilder()
                .set( log_queries, VERBOSE )
                .set( log_queries_parameter_logging_enabled, false )
                .set( log_queries_runtime_logging_enabled, false )
                .set( log_queries_allocation_logging_enabled, false )
                .build();
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, config );
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
        assertNotEquals( query1.internalQueryId(), query2.internalQueryId(), "Queries should have different ids");
        assertLog( logFormat, outContent.toString() ).contains(
                line()
                        .level( Level.INFO )
                        .started()
                        .elapsed( 0 )
                        .queryId( query1.internalQueryId() )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ),
                line()
                        .level( Level.INFO )
                        .started()
                        .elapsed( 5 )
                        .queryId( query2.internalQueryId() )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ),
                line()
                        .level( Level.INFO )
                        .elapsed( 10 )
                        .queryId( query2.internalQueryId() )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ),
                line()
                        .level( Level.INFO )
                        .elapsed( 15 )
                        .queryId( query1.internalQueryId() )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ) );
    }

    @ParameterizedTest
    @EnumSource( FormattedLogFormat.class )
    void includeReasonForFailure( FormattedLogFormat logFormat )
    {
        Config config = Config.newBuilder()
                              .set( log_queries, VERBOSE )
                              .set( log_queries_parameter_logging_enabled, false )
                              .set( log_queries_runtime_logging_enabled, false )
                              .set( log_queries_allocation_logging_enabled, false )
                              .build();
        ConfiguredQueryLogger queryLogger = queryLogger( logFormat, config );

        // when
        ExecutingQuery query = query( SESSION_1, "TestUser", QUERY_1 );
        query.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );

        queryLogger.start( query );
        clock.forward( 10, TimeUnit.MILLISECONDS );
        queryLogger.failure( query, "my reason" );

        // then
        assertLog( logFormat, outContent.toString() ).contains(
                line()
                        .level( Level.INFO )
                        .started()
                        .elapsed( 0 )
                        .queryId( query.internalQueryId() )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 ),
                line()
                        .level( Level.ERROR )
                        .elapsed( 10 )
                        .queryId( query.internalQueryId() )
                        .source( SESSION_1.asConnectionDetails() )
                        .database( defaultDbId.name() )
                        .user( "TestUser" )
                        .query( QUERY_1 )
                        .reason( "my reason" ) );
    }

    private ConfiguredQueryLogger queryLogger( FormattedLogFormat logFormat )
    {
        Config config = Config.newBuilder()
                .set( log_queries_parameter_logging_enabled, false )
                .set( log_queries_allocation_logging_enabled, false )
                .build();

        return queryLogger( logFormat, config );
    }

    private ConfiguredQueryLogger queryLogger( FormattedLogFormat logFormat, Config config )
    {
        Log4jLogProvider logProvider = new Log4jLogProvider(
                LogConfig.createBuilder( outContent, Level.INFO )
                         .withFormat( logFormat )
                         .withCategory( false )
                         .build() );
        config.set( log_queries_threshold, Duration.ofMillis( thresholdInMillis ) );
        if ( !config.isExplicitlySet( log_queries ) )
        {
            config.set( log_queries, GraphDatabaseSettings.LogQueryLevel.INFO );
        }
        if ( !config.isExplicitlySet( log_queries_runtime_logging_enabled ) )
        {
            config.set( log_queries_runtime_logging_enabled, false );
        }
        return new ConfiguredQueryLogger( (LogExtended) logProvider.getLog( getClass() ), config );
    }

    private ExecutingQuery query( ClientConnectionInfo sessionInfo, String username, String queryText )
    {
        return query( sessionInfo, defaultDbId, username, queryText );
    }

    private ExecutingQuery query( ClientConnectionInfo sessionInfo, NamedDatabaseId namedDatabaseId, String username, String queryText )
    {
        return query( sessionInfo, namedDatabaseId, username, queryText, emptyMap(), emptyMap() );
    }

    private ExecutingQuery query( ClientConnectionInfo sessionInfo, NamedDatabaseId namedDatabaseId, String username, String queryText,
            Map<String,Object> params, Map<String,Object> metaData )
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
                cpuClock,
                true );
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
            registerComponent( SecurityContext.class );

            registerProcedure( UserManagementProcedures.class );
        }
    }

    private static ContentValidator assertLog( FormattedLogFormat format, String content )
    {
        switch ( format )
        {
        case STANDARD_FORMAT:
            return new LoggerContentValidator( content );
        case JSON_FORMAT:
            return new JsonContentValidator( content );
        default:
        }
        throw new AssertionError();
    }

    private interface ContentValidator
    {
        void contains( LogLineContent... logLines );
        void contains( LogLineContent logLine, Throwable exception );
    }

    private static class JsonContentValidator implements ContentValidator
    {
        private final String[] contentLines;

        private JsonContentValidator( String contentLines )
        {
            this.contentLines = contentLines.split( System.lineSeparator() );
        }

        @Override
        public void contains( LogLineContent... logLines )
        {
            try
            {
                assertThat( logLines.length ).isEqualTo( contentLines.length );
                for ( int i = 0; i < logLines.length; i++ )
                {
                    LogLineContent expected = logLines[i];
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, String> map = mapper.readValue( contentLines[i], new TypeReference<>()
                    { } );
                    assertLine( expected, map );
                }
            }
            catch ( JsonProcessingException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void contains( LogLineContent logLine, Throwable exception )
        {
            try
            {
                assertThat( contentLines.length ).isEqualTo( 1 );

                ObjectMapper mapper = new ObjectMapper();
                Map<String, String> map = mapper.readValue( contentLines[0], new TypeReference<>()
                { } );
                assertLine( logLine, map );
                assertThat( map.get( "stacktrace" ) ).contains( exception.toString() );
            }
            catch ( JsonProcessingException e )
            {
                throw new RuntimeException( e );
            }
        }

        private void assertLine( LogLineContent expected, Map<String,String> map )
        {
            assertEquals( expected.expectedLevel, map.get( "level" ), "'level' mismatch" );
            assertEquals( expected.expectedStarted, map.get( "event" ).equals( "start" ), "'started' mismatch"  );
            assertEquals( expected.expectedQueryId, map.get( "id" ), "'id' mismatch"  );
            assertEquals( expected.expectedElapsed, map.get( "elapsedTimeMs" ), "'elapsed' mismatch"  );
            assertEquals( expected.expectedPlanning, map.get( "planning" ), "'planning' mismatch"  );
            assertEquals( expected.expectedCpu, map.get( "cpu" ), "'cpu' mismatch"  );
            assertEquals( expected.expectedWaiting, map.get( "waiting" ), "'waiting' mismatch"  );
            assertEquals( expected.expectedAllocatedBytes, map.get( "allocatedBytes" ), "'allocatedBytes' mismatch"  );
            assertEquals( expected.expectedPageHits, map.get( "pageHits" ), "'pageHits' mismatch"  );
            assertEquals( expected.expectedPageFaults, map.get( "pageFaults" ), "'pageFaults' mismatch"  );
            assertEquals( expected.expectedSource, map.get( "source" ), "'source' mismatch"  );
            assertEquals( expected.expectedDatabase, map.get( "database" ), "'database' mismatch"  );
            assertEquals( expected.expectedUser, map.get( "username" ), "'user' mismatch"  );
            assertEquals( expected.expectedQuery, map.get( "query" ), "'query' mismatch"  );
            assertEquals( expected.expectedParams, map.get( "queryParameters" ), "'params' mismatch"  );
            assertEquals( expected.expectedRuntime, map.get( "runtime" ), "'runtime' mismatch"  );
            assertEquals( expected.expectedAdditional, map.get( "annotationData" ), "'additional' mismatch"  );
            assertEquals( expected.expectedReason, map.get( "failureReason" ), "'reason' mismatch"  );
        }
    }

    private static final Pattern LOGGER_LINE_PARSER = Pattern.compile(
            "^(?<time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{4}) " +
                    "(?<level>\\w{4,5})\\s{1,2}" +
                    "(?<started>Query started: )?" +
                    "(?:id:(?<id>\\d+) - )?" +
                    "(?<elapsed>\\d+) ms: " +
                    "(?:\\(planning: (?<planning>\\d+)(?:, cpu: (?<cpu>\\d+))?, waiting: (?<waiting>\\d+)\\) - )?" +
                    "(?:(?<allocatedBytes>\\d+) B - )?" +
                    "(?:(?<pageHits>\\d+) page hits, (?<pageFaults>\\d+) page faults - )?" +
                    "(?<source>embedded-session\\t|bolt-session[^>]*>|server-session(?:\\t[^\\t]*){3})\\t" +
                    "(?<database>[^\\s]+) - " +
                    "(?<user>[^\\s]+) - " +
                    "(?<query>.+?(?= - ))" +
                    "(?: - (?<params>\\{.*?(?=} - )}))?" +
                    "(?: - runtime=(?<runtime>\\w+))? - " +
                    "(?<additional>\\{.+?(?=(?:$| - )))" +
                    "(?: - (?<reason>.*$))?$" );

    private static class LoggerContentValidator implements ContentValidator
    {
        private final String[] contentLines;

        LoggerContentValidator( String content )
        {
            contentLines = content.split( System.lineSeparator() );
        }

        @Override
        public void contains( LogLineContent... logLines )
        {
            assertThat( logLines.length ).isEqualTo( contentLines.length );
            for ( int i = 0; i < logLines.length; i++ )
            {
                assertLine( contentLines[i], logLines[i] );
            }
        }

        @Override
        public void contains( LogLineContent logLine, Throwable exception )
        {
            assertThat( contentLines.length ).isGreaterThan( 1 );
            assertLine( contentLines[0], logLine );
            assertThat( contentLines[1] ).contains( exception.toString() ); // Only verify first line of the exception.
        }

        private void assertLine(  String contentLine, LogLineContent expected )
        {
            Matcher matcher = LOGGER_LINE_PARSER.matcher( contentLine );
            assertTrue( matcher.matches() );
            assertEquals( expected.expectedLevel, matcher.group( "level" ), "'level' mismatch" );
            assertEquals( expected.expectedStarted, matcher.group( "started" ) != null, "'started' mismatch"  );
            assertEquals( expected.expectedQueryId, matcher.group( "id" ), "'id' mismatch"  );
            assertEquals( expected.expectedElapsed, matcher.group( "elapsed" ), "'elapsed' mismatch"  );
            assertEquals( expected.expectedPlanning, matcher.group( "planning" ), "'planning' mismatch"  );
            assertEquals( expected.expectedCpu, matcher.group( "cpu" ), "'cpu' mismatch"  );
            assertEquals( expected.expectedWaiting, matcher.group( "waiting" ), "'waiting' mismatch"  );
            assertEquals( expected.expectedAllocatedBytes, matcher.group( "allocatedBytes" ), "'allocatedBytes' mismatch"  );
            assertEquals( expected.expectedPageHits, matcher.group( "pageHits" ), "'pageHits' mismatch"  );
            assertEquals( expected.expectedPageFaults, matcher.group( "pageFaults" ), "'pageFaults' mismatch"  );
            assertEquals( expected.expectedSource, matcher.group( "source" ), "'source' mismatch"  );
            assertEquals( expected.expectedDatabase, matcher.group( "database" ), "'database' mismatch"  );
            assertEquals( expected.expectedUser, matcher.group( "user" ), "'user' mismatch"  );
            assertEquals( expected.expectedQuery, matcher.group( "query" ), "'query' mismatch"  );
            assertEquals( expected.expectedParams, matcher.group( "params" ), "'params' mismatch"  );
            assertEquals( expected.expectedRuntime, matcher.group( "runtime" ), "'runtime' mismatch"  );
            assertEquals( expected.expectedAdditional, matcher.group( "additional" ), "'additional' mismatch"  );
            assertEquals( expected.expectedReason, matcher.group( "reason" ), "'reason' mismatch"  );
        }
    }

    private static LogLineContent line()
    {
        return new LogLineContent();
    }

    private static class LogLineContent
    {
        String expectedLevel;
        boolean expectedStarted;
        String expectedQueryId;
        String expectedElapsed;
        String expectedPlanning;
        String expectedCpu;
        String expectedWaiting;
        String expectedAllocatedBytes;
        String expectedPageHits;
        String expectedPageFaults;
        String expectedSource;
        String expectedDatabase;
        String expectedUser;
        String expectedQuery;
        String expectedParams;
        String expectedRuntime;
        String expectedAdditional = "{}";
        String expectedReason;

        LogLineContent level( Level level )
        {
            this.expectedLevel = level.toString();
            return this;
        }
        LogLineContent started()
        {
            this.expectedStarted = true;
            return this;
        }
        LogLineContent queryId( long queryId )
        {
            this.expectedQueryId = String.valueOf( queryId );
            return this;
        }
        LogLineContent elapsed( long elapsed )
        {
            this.expectedElapsed = String.valueOf( elapsed );
            return this;
        }
        LogLineContent planning( long planning )
        {
            this.expectedPlanning = String.valueOf( planning );
            return this;
        }
        LogLineContent cpu( long cpu )
        {
            this.expectedCpu = String.valueOf( cpu );
            return this;
        }
        LogLineContent waiting( long waiting )
        {
            this.expectedWaiting = String.valueOf( waiting );
            return this;
        }
        LogLineContent allocatedBytes( long allocatedBytes )
        {
            this.expectedAllocatedBytes = String.valueOf( allocatedBytes );
            return this;
        }
        LogLineContent pageHits( long pageHits )
        {
            this.expectedPageHits = String.valueOf( pageHits );
            return this;
        }
        LogLineContent pageFaults( long pageFaults )
        {
            this.expectedPageFaults = String.valueOf( pageFaults );
            return this;
        }
        LogLineContent source( String source )
        {
            this.expectedSource = source;
            return this;
        }
        LogLineContent database( String database )
        {
            this.expectedDatabase = database;
            return this;
        }
        LogLineContent user( String user )
        {
            this.expectedUser = user;
            return this;
        }
        LogLineContent query( String query )
        {
            this.expectedQuery = query;
            return this;
        }
        LogLineContent params( String params )
        {
            this.expectedParams = params;
            return this;
        }
        LogLineContent runtime( String runtime )
        {
            this.expectedRuntime = runtime;
            return this;
        }
        LogLineContent additional( String additional )
        {
            this.expectedAdditional = additional;
            return this;
        }
        LogLineContent reason( String reason )
        {
            this.expectedReason = reason;
            return this;
        }
    }
}
