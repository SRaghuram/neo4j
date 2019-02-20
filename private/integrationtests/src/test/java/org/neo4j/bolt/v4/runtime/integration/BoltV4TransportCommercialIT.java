/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.bolt.v4.runtime.integration;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v4.messaging.PullNMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newMessageEncoder;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newNeo4jPack;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

public class BoltV4TransportCommercialIT
{
    private static final String USER_AGENT = "TestClient/4.0";

    @Rule
    public final Neo4jWithSocket server =
            new Neo4jWithSocket( getClass(), new TestCommercialGraphDatabaseFactory(), settings -> settings.put( auth_enabled.name(), "false" ) );

    private HostnamePort address;
    private TransportConnection connection;
    private TransportTestUtil util;

    @Before
    public void setUp()
    {
        address = server.lookupDefaultConnector();
        connection = new SocketConnection();
        util = new TransportTestUtil( newNeo4jPack(), newMessageEncoder() );

        GraphDatabaseService gds = server.graphDatabaseService();
        try ( Transaction tx = gds.beginTx() )
        {
            for ( int i = 30; i <= 40; i++ )
            {
                gds.createNode( Label.label( "L" + i ) );
            }
            tx.success();
        }
    }

    @After
    public void tearDown() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    @Test
    public void shouldStreamWhenStatementIdNotProvided() throws Exception
    {
        negotiateBoltV4();

        for ( String runtime : RUNTIMES )
        {
            String query = "CYPHER runtime=" + runtime + " UNWIND $param AS x RETURN x";

            // begin a transaction
            connection.send( util.chunk( new BeginMessage( VirtualValues.EMPTY_MAP ) ) );
            assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

            // execute a query
            connection.send( util.chunk( new RunMessage( query, paramWithRange( 30, 40 ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgSuccess( allOf( hasEntry( is( "stmt_id" ), equalTo( 0L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

            // request 5 records but do not provide stmt_id
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 5L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 30L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 31L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 32L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 33L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 34L ) ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 2 more records but do not provide stmt_id
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 2L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 35L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 36L ) ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 3 more records and provide stmt_id
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 3L, "stmt_id", 0L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 37L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 38L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 39L ) ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 10 more records but do not provide stmt_id, only 1 more record is available
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 10L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 40L ) ) ) ),
                    msgSuccess( allOf( not( hasKey( "has_more" ) ), hasKey( "t_last" ) ) ) ) );

            // rollback the transaction
            connection.send( util.chunk( RollbackMessage.ROLLBACK_MESSAGE ) );
            assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
        }
    }

    @Test
    public void shouldStreamWhenStatementIdNotProvidedWithStandaloneProcedureCall() throws Exception
    {
        negotiateBoltV4();

        // begin a transaction
        connection.send( util.chunk( new BeginMessage( VirtualValues.EMPTY_MAP ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // execute a query
        connection.send( util.chunk( new RunMessage( "CALL db.labels()" ) ) ); // Standalone procedure call
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( hasEntry( is( "stmt_id" ), equalTo( 0L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

        // request 5 records but do not provide stmt_id
        connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 5L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( stringValue( "L30" ) ), equalTo( longValue( 1 ) ) ) ),
                msgRecord( eqRecord( equalTo( stringValue( "L31" ) ), equalTo( longValue( 1 ) ) ) ),
                msgRecord( eqRecord( equalTo( stringValue( "L32" ) ), equalTo( longValue( 1 ) ) ) ),
                msgRecord( eqRecord( equalTo( stringValue( "L33" ) ), equalTo( longValue( 1 ) ) ) ),
                msgRecord( eqRecord( equalTo( stringValue( "L34" ) ), equalTo( longValue( 1 ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 2 more records but do not provide stmt_id
        connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 2L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( stringValue( "L35" ) ), equalTo( longValue( 1 ) ) ) ),
                msgRecord( eqRecord( equalTo( stringValue( "L36" ) ), equalTo( longValue( 1 ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 3 more records and provide stmt_id
        connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 3L, "stmt_id", 0L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( stringValue( "L37" ) ), equalTo( longValue( 1 ) ) ) ),
                msgRecord( eqRecord( equalTo( stringValue( "L38" ) ), equalTo( longValue( 1 ) ) ) ),
                msgRecord( eqRecord( equalTo( stringValue( "L39" ) ), equalTo( longValue( 1 ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 10 more records but do not provide stmt_id, only 1 more record is available
        connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 10L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( stringValue( "L40" ) ), equalTo( longValue( 1 ) ) ) ),
                msgSuccess( allOf( not( hasKey( "has_more" ) ), hasKey( "t_last" ) ) ) ) );

        // rollback the transaction
        connection.send( util.chunk( RollbackMessage.ROLLBACK_MESSAGE ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldSendAndReceiveStatementIds() throws Exception
    {
        negotiateBoltV4();

        for ( String runtime : RUNTIMES )
        {
            String query = "CYPHER runtime=" + runtime + " UNWIND $param AS x RETURN x";

            // begin a transaction
            connection.send( util.chunk( new BeginMessage( VirtualValues.EMPTY_MAP ) ) );
            assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

            // execute query #0
            connection.send( util.chunk( new RunMessage( query, paramWithRange( 1, 10 ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgSuccess( allOf( hasEntry( is( "stmt_id" ), equalTo( 0L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

            // request 3 records for query #0
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 3L, "stmt_id", 0L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 2L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 3L ) ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // execute query #1
            connection.send( util.chunk( new RunMessage( query, paramWithRange(11, 20) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgSuccess( allOf( hasEntry( is( "stmt_id" ), equalTo( 1L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

            // request 2 records for query #1
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 2L, "stmt_id", 1L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 11L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 12L ) ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // execute query #2
            connection.send( util.chunk( new RunMessage( query, paramWithRange(21, 30) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgSuccess( allOf( hasEntry( is( "stmt_id" ), equalTo( 2L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

            // request 4 records for query #2
            // no stmt_id - should use the statement from the latest RUN
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 4L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 21L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 22L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 23L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 24L ) ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // execute query #3
            connection.send( util.chunk( new RunMessage( query, paramWithRange(31, 40) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgSuccess( allOf( hasEntry( is( "stmt_id" ), equalTo( 3L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

            // request 1 record for query #3
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 1L, "stmt_id", 3L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 31L ) ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 2 records for query #0
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 2L, "stmt_id", 0L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 4L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 5L ) ) ) ),
                    msgSuccess( singletonMap( "has_more", true ) ) ) );

            // request 9 records for query #3
            connection.send( util.chunk( new PullNMessage( asMapValue( map( "n", 9L, "stmt_id", 3L ) ) ) ) );
            assertThat( connection, util.eventuallyReceives(
                    msgRecord( eqRecord( equalTo( longValue( 32L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 33L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 34L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 35L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 36L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 37L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 38L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 39L ) ) ) ),
                    msgRecord( eqRecord( equalTo( longValue( 40L ) ) ) ),
                    msgSuccess( allOf( not( hasKey( "has_more" ) ), hasKey( "t_last" ) ) ) ) );

            // commit the transaction
            connection.send( util.chunk( CommitMessage.COMMIT_MESSAGE ) );
            assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
        }
    }

    private static final String[] RUNTIMES = new String[]{ "interpreted", "slotted", "compiled", "morsel debug=singleThreaded", "morsel" };

    private static MapValue paramWithRange( int from, int to )
    {
        return VirtualValues.map( new String[]{"param"}, new AnyValue[]{VirtualValues.range( from, to, 1 )} );
    }

    private void negotiateBoltV4() throws Exception
    {
        connection.connect( address ).send( util.acceptedVersions( 4, 0, 0, 0 ) );
        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 4} ) );

        connection.send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }
}
