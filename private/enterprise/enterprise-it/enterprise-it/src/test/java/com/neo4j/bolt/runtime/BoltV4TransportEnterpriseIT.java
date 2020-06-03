/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.runtime;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.PullMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgRecord;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.StreamConditions.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newMessageEncoder;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.DB_NAME_KEY;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.map;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class BoltV4TransportEnterpriseIT
{
    private static final String USER_AGENT = "TestClient/4.0";

    @Inject
    public Neo4jWithSocket server;

    private HostnamePort address;
    private TransportConnection connection;
    private TransportTestUtil util;

    private static Stream<Arguments> argumentsProvider()
    {
        return Stream.of( Arguments.of( SocketConnection.class ), Arguments.of( SecureSocketConnection.class ) );
    }

    private void init( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        connection = connectionClass.getDeclaredConstructor().newInstance();
    }

    @BeforeEach
    public void setup( TestInfo testInfo ) throws Exception
    {
        server.setGraphDatabaseFactory( new TestEnterpriseDatabaseManagementServiceBuilder() );
        server.setConfigure( withOptionalBoltEncryption() );
        server.init( testInfo );

        address = server.lookupDefaultConnector();
        //connection = connectionClass.newInstance();
        util = new TransportTestUtil( newMessageEncoder() );

        GraphDatabaseService gds = server.graphDatabaseService();
        try ( Transaction tx = gds.beginTx() )
        {
            for ( int i = 30; i <= 40; i++ )
            {
                tx.createNode( Label.label( "L" + i ) );
            }
            tx.commit();
        }
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldStreamWhenStatementIdNotProvided( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );
        negotiateBoltV4();

        for ( String runtime : READ_RUNTIMES )
        {
            String query = "CYPHER runtime=" + runtime + " UNWIND $param AS x RETURN x";

            // begin a transaction
            connection.send( util.chunk( new BeginMessage() ) );
            assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

            // execute a query
            connection.send( util.chunk( new RunMessage( query, paramWithRange( 30, 40 ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgSuccess( message -> assertThat( message )
                            .containsEntry(  "qid",  0L ).containsKeys( "fields", "t_first" ) ) ) );

            // request 5 records but do not provide qid
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 5L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 30L ) ) ),
                    msgRecord( eqRecord( longEquals( 31L ) ) ),
                    msgRecord( eqRecord( longEquals( 32L ) ) ),
                    msgRecord( eqRecord( longEquals( 33L ) ) ),
                    msgRecord( eqRecord( longEquals( 34L ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 2 more records but do not provide qid
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 35L ) ) ),
                    msgRecord( eqRecord( longEquals( 36L ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 3 more records and provide qid
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 3L, "qid", 0L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 37L ) ) ),
                    msgRecord( eqRecord( longEquals( 38L ) ) ),
                    msgRecord( eqRecord( longEquals( 39L ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 10 more records but do not provide qid, only 1 more record is available
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 10L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 40L ) ) ),
                    msgSuccess( message -> assertThat( message )
                            .containsKey( "t_last" )
                            .doesNotContainKey( "has_more" ) ) ) );

            // rollback the transaction
            connection.send( util.chunk( RollbackMessage.ROLLBACK_MESSAGE ) );
            assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        }
    }

    private Condition<AnyValue> longEquals( long expected )
    {
        return new Condition<>( value -> value.equals( longValue( expected ) ), "long equals" );
    }

    private Condition<AnyValue> stringEquals( String expected )
    {
        return new Condition<>( value -> value.equals( stringValue( expected ) ), "String equals" );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldStreamWhenStatementIdNotProvidedWithStandaloneProcedureCall( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );
        negotiateBoltV4();

        // begin a transaction
        connection.send( util.chunk( new BeginMessage() ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // execute a query
        connection.send( util.chunk( new RunMessage( "CALL db.labels()" ) ) ); // Standalone procedure call
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess( message -> assertThat( message )
                        .containsEntry( "qid", 0L ).containsKeys( "fields", "t_first" ) ) ) );

        // request 5 records but do not provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 5L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( stringEquals( "L30" ) ) ),
                msgRecord( eqRecord( stringEquals( "L31" ) ) ),
                msgRecord( eqRecord( stringEquals( "L32" ) ) ),
                msgRecord( eqRecord( stringEquals( "L33" ) ) ),
                msgRecord( eqRecord( stringEquals( "L34" ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 2 more records but do not provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( stringEquals( "L35" ) ) ),
                msgRecord( eqRecord( stringEquals( "L36" ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 3 more records and provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 3L, "qid", 0L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( stringEquals( "L37" ) ) ),
                msgRecord( eqRecord( stringEquals( "L38" ) ) ),
                msgRecord( eqRecord( stringEquals( "L39" ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 10 more records but do not provide qid, only 1 more record is available
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 10L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( stringEquals( "L40" ) ) ),
                msgSuccess( message -> assertThat( message ).doesNotContainKey("has_more" )
                        .containsKey( "t_last" ) ) ) );

        // rollback the transaction
        connection.send( util.chunk( RollbackMessage.ROLLBACK_MESSAGE ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSendAndReceiveStatementIds( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );
        negotiateBoltV4();

        for ( String runtime : READ_RUNTIMES )
        {
            String query = "CYPHER runtime=" + runtime + " UNWIND $param AS x RETURN x";

            // begin a transaction
            connection.send( util.chunk( new BeginMessage() ) );
            assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

            // execute query #0
            connection.send( util.chunk( new RunMessage( query, paramWithRange( 1, 10 ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess(
                    message -> assertThat( message ).containsEntry( "qid", 0L ).containsKeys( "fields", "t_first" ) ) ) );

            // request 3 records for query #0
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 3L, "qid", 0L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 1L ) ) ),
                    msgRecord( eqRecord( longEquals( 2L ) ) ),
                    msgRecord( eqRecord( longEquals( 3L ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // execute query #1
            connection.send( util.chunk( new RunMessage( query, paramWithRange(11, 20) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgSuccess( message -> assertThat( message ).containsEntry( "qid", 1L ).containsKeys( "fields", "t_first" ) ) ) );

            // request 2 records for query #1
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L, "qid", 1L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 11L ) ) ),
                    msgRecord( eqRecord( longEquals( 12L ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // execute query #2
            connection.send( util.chunk( new RunMessage( query, paramWithRange(21, 30) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgSuccess( message -> assertThat( message ).containsEntry( "qid", 2L ).containsKeys( "fields", "t_first" ) ) ) );

            // request 4 records for query #2
            // no qid - should use the statement from the latest RUN
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 4L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 21L ) ) ),
                    msgRecord( eqRecord( longEquals( 22L ) ) ),
                    msgRecord( eqRecord( longEquals( 23L ) ) ),
                    msgRecord( eqRecord( longEquals( 24L ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // execute query #3
            connection.send( util.chunk( new RunMessage( query, paramWithRange(31, 40) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgSuccess( message -> assertThat( message ).containsEntry( "qid", 3L ).containsKeys( "fields", "t_first" )) ) );

            // request 1 record for query #3
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 1L, "qid", 3L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 31L ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 2 records for query #0
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L, "qid", 0L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 4L ) ) ),
                    msgRecord( eqRecord( longEquals( 5L ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 9 records for query #3
            connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 9L, "qid", 3L ) ) ) ) );
            assertThat( connection ).satisfies( util.eventuallyReceives(
                    msgRecord( eqRecord( longEquals( 32L ) ) ),
                    msgRecord( eqRecord( longEquals( 33L ) ) ),
                    msgRecord( eqRecord( longEquals( 34L ) ) ),
                    msgRecord( eqRecord( longEquals( 35L ) ) ),
                    msgRecord( eqRecord( longEquals( 36L ) ) ),
                    msgRecord( eqRecord( longEquals( 37L ) ) ),
                    msgRecord( eqRecord( longEquals( 38L ) ) ),
                    msgRecord( eqRecord( longEquals( 39L ) ) ),
                    msgRecord( eqRecord( longEquals( 40L ) ) ),
                    msgSuccess( message -> assertThat( message )
                            .doesNotContainKey( "has_more" ).containsKey( "t_last" ) ) ) );

            // commit the transaction
            connection.send( util.chunk( CommitMessage.COMMIT_MESSAGE ) );
            assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleQueryWithProfile( Class<? extends TransportConnection> connectionClass ) throws Throwable
    {
        init( connectionClass );

        //Given
        negotiateBoltV4();

        for ( String runtime : RUNTIMES )
        {
            // When
            String query = String.format("CYPHER runtime=%s PROFILE MERGE (n {name: 'bob'}) ON CREATE SET n.created = timestamp() ON " +
                           "MATCH SET n.counter = coalesce(n.counter, 0) + 1", runtime);

            //begin, run, pull, rollback
            connection.send( util.chunk(
                    new BeginMessage(),
                    new RunMessage( query ),
                    new PullMessage( asMapValue( map( "n", 5L ) ) ),
                    RollbackMessage.ROLLBACK_MESSAGE ) );

            // Then
            assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess() ) );
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldBeAbleToRunOnSelectedDatabase( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );
        negotiateBoltV4();

        DatabaseManagementService managementService = server.getManagementService();
        managementService.createDatabase( "first" );
        managementService.createDatabase( "second" );

        // create a node
        sessionRun( "CREATE (n{ name: 'Molly'}) RETURN n.name", "first", stringValue( "Molly" ) );

        // Then we can query this just created node on the same database
        sessionRun( "MATCH (n) WHERE n.name = 'Molly' RETURN count(n)", "first", longValue( 1L ) );
        // Then we cannot query this just created node on a different database
        sessionRun( "MATCH (n) WHERE n.name = 'Molly' RETURN count(n)", "second", longValue( 0L ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldBeAbleToRunOnSelectedDatabaseInTransaction( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );
        negotiateBoltV4();

        DatabaseManagementService managementService = server.getManagementService();
        managementService.createDatabase( "first" );
        managementService.createDatabase( "second" );

        // create a node
        transactionRun( "CREATE (n{ name: 'Molly'}) RETURN n.name", "first", stringValue( "Molly" ) );

        // Then we can query this just created node on the same database
        transactionRun( "MATCH (n) WHERE n.name = 'Molly' RETURN count(n)", "first", longValue( 1L ) );
        // Then we cannot query this just created node on a different database
        transactionRun( "MATCH (n) WHERE n.name = 'Molly' RETURN count(n)", "second", longValue( 0L ) );
    }

    private void sessionRun( String query, String databaseName, AnyValue expected ) throws Exception
    {
        var runMetadata = map( new String[]{DB_NAME_KEY}, new AnyValue[]{stringValue( databaseName )} );
        connection.send( util.chunk( new RunMessage( query, EMPTY_MAP, runMetadata, List.of(), null, AccessMode.WRITE, Map.of(), databaseName ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsKeys( "fields", "t_first" ) ) ) );

        // "pull all"
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 100L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( new Condition<>( v -> v.equals( expected ), "Equal" ) ) ),
                msgSuccess( message -> assertThat( message ).doesNotContainKey( "has_more" ).containsKey( "t_last" ) ) ) );
    }

    private void transactionRun( String query, String databaseName, AnyValue expected ) throws Exception
    {
        // begin a transaction
        var beginMetadata = map( new String[]{DB_NAME_KEY}, new AnyValue[]{stringValue( databaseName )} );
        connection.send( util.chunk( new BeginMessage( beginMetadata, List.of(), null, AccessMode.WRITE, Map.of(), databaseName ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // run
        connection.send( util.chunk( new RunMessage( query ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "qid", 0L ).containsKeys( "fields", "t_first" ) ) ) );

        // "pull all"
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 100L, "qid", 0L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( new Condition<>( v -> v.equals( expected ), "Equal" ) ) ),
                msgSuccess( message -> assertThat( message ).doesNotContainKey("has_more" ).containsKey( "t_last" ) ) ) );

        // commit the transaction
        connection.send( util.chunk( CommitMessage.COMMIT_MESSAGE ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private static final String[] RUNTIMES = new String[]{ "interpreted", "slotted", "legacy_compiled" };
    private static final String[] READ_RUNTIMES = new String[]{ "interpreted", "slotted", "legacy_compiled", "pipelined" };

    private static MapValue paramWithRange( int from, int to )
    {
        return map( new String[]{"param"}, new AnyValue[]{VirtualValues.range( from, to, 1 )} );
    }

    private void negotiateBoltV4() throws Exception
    {
        connection.connect( address ).send( util.acceptedVersions( 4, 0, 0, 0 ) );
        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 0, 4} ) );

        connection.send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }
}
