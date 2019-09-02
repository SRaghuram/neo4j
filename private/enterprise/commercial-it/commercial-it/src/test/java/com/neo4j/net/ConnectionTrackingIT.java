/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.net;

import com.neo4j.harness.internal.CommercialInProcessNeo4jBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static com.neo4j.net.ConnectionTrackingIT.TestConnector.BOLT;
import static com.neo4j.net.ConnectionTrackingIT.TestConnector.HTTP;
import static com.neo4j.net.ConnectionTrackingIT.TestConnector.HTTPS;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.bolt.testing.MessageMatchers.msgRecord;
import static org.neo4j.bolt.testing.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.testing.StreamMatchers.eqRecord;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;
import static org.neo4j.server.configuration.ServerSettings.webserver_max_threads;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.server.HTTP.RawPayload;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;
import static org.neo4j.test.server.HTTP.Response;
import static org.neo4j.test.server.HTTP.withBasicAuth;
import static org.neo4j.values.storable.Values.stringOrNoValue;
import static org.neo4j.values.storable.Values.stringValue;

@TestDirectoryExtension
class ConnectionTrackingIT
{
    private static final String NEO4J_USER_PWD = "test";
    private static final String OTHER_USER = "otherUser";
    private static final String OTHER_USER_PWD = "test";

    private static final List<String> LIST_CONNECTIONS_PROCEDURE_COLUMNS = Arrays.asList(
            "connectionId", "connectTime", "connector", "username", "userAgent", "serverAddress", "clientAddress" );

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Set<TransportConnection> connections = ConcurrentHashMap.newKeySet();
    private final TransportTestUtil util = new TransportTestUtil();

    @Inject
    private TestDirectory dir;

    private GraphDatabaseAPI db;
    private InProcessNeo4j neo4j;
    private long dummyNodeId;

    @BeforeEach
    void setUp()
    {
        neo4j = new CommercialInProcessNeo4jBuilder( dir.directory() )
                .withConfig( neo4j_home, dir.directory().toPath().toAbsolutePath() )
                .withConfig( auth_enabled, true )
                .withConfig( HttpConnector.enabled, true )
                .withConfig( HttpsConnector.enabled, true )
                .withConfig( webserver_max_threads, 50 ) /* higher than the amount of concurrent requests tests execute*/
                .withConfig( online_backup_enabled, false )
                .build();
        neo4j.start();
        db = (GraphDatabaseAPI) neo4j.defaultDatabaseService();

        changeDefaultPasswordForUserNeo4j( NEO4J_USER_PWD );
        createNewUser( OTHER_USER, OTHER_USER_PWD );
        dummyNodeId = createDummyNode();

        closeAcceptedConnections();
    }

    @AfterEach
    void afterEach() throws Exception
    {
        for ( TransportConnection connection : connections )
        {
            try
            {
                connection.disconnect();
            }
            catch ( Exception ignore )
            {
            }
        }
        closeAcceptedConnections();
        executor.shutdownNow();
        terminateAllTransactions();
        awaitNumberOfAcceptedConnectionsToBe( 0 );

        neo4j.close();
    }

    private void closeAcceptedConnections()
    {
        acceptedConnectionsFromConnectionTracker().forEach( TrackedNetworkConnection::close );
    }

    @Test
    void shouldListNoConnectionsWhenIdle() throws Exception
    {
        verifyConnectionCount( HTTP, null, 0 );
        verifyConnectionCount( HTTPS, null, 0 );
        verifyConnectionCount( BOLT, null, 0 );
    }
    @Test
    void shouldListUnauthenticatedHttpConnections() throws Exception
    {
        testListingOfUnauthenticatedConnections( 5, 0, 0 );
    }

    @Test
    void shouldListUnauthenticatedHttpsConnections() throws Exception
    {
        testListingOfUnauthenticatedConnections( 0, 2, 0 );
    }

    @Test
    void shouldListUnauthenticatedBoltConnections() throws Exception
    {
        testListingOfUnauthenticatedConnections( 0, 0, 4 );
    }

    @Test
    void shouldListUnauthenticatedConnections() throws Exception
    {
        testListingOfUnauthenticatedConnections( 3, 2, 7 );
    }

    @Test
    void shouldListAuthenticatedHttpConnections() throws Exception
    {
        lockNodeAndExecute( dummyNodeId, () ->
        {
            for ( int i = 0; i < 4; i++ )
            {
                updateNodeViaHttp( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }
            for ( int i = 0; i < 3; i++ )
            {
                updateNodeViaHttp( dummyNodeId, OTHER_USER, OTHER_USER_PWD );
            }

        } );
        awaitNumberOfAuthenticatedConnectionsToBe( 7 );
        verifyAuthenticatedConnectionCount( HTTP, "neo4j", 4 );
        verifyAuthenticatedConnectionCount( HTTP, OTHER_USER, 3 );
    }

    @Test
    void shouldListAuthenticatedHttpsConnections() throws Exception
    {
        lockNodeAndExecute( dummyNodeId, () ->
        {
            for ( int i = 0; i < 4; i++ )
            {
                updateNodeViaHttps( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }
            for ( int i = 0; i < 5; i++ )
            {
                updateNodeViaHttps( dummyNodeId, OTHER_USER, OTHER_USER_PWD );
            }

            awaitNumberOfAuthenticatedConnectionsToBe( 9 );
        } );
        verifyAuthenticatedConnectionCount( HTTPS, "neo4j", 4 );
        verifyAuthenticatedConnectionCount( HTTPS, OTHER_USER, 5 );
    }

    @Test
    void shouldListAuthenticatedBoltConnections() throws Exception
    {
        lockNodeAndExecute( dummyNodeId, () ->
        {
            for ( int i = 0; i < 2; i++ )
            {
                updateNodeViaBolt( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }
            for ( int i = 0; i < 5; i++ )
            {
                updateNodeViaBolt( dummyNodeId, OTHER_USER, OTHER_USER_PWD );
            }

        } );
        awaitNumberOfAuthenticatedConnectionsToBe( 7 );
        verifyAuthenticatedConnectionCount( BOLT, "neo4j", 2 );
        verifyAuthenticatedConnectionCount( BOLT, OTHER_USER, 5 );
    }

    @Test
    void shouldListAuthenticatedConnections() throws Exception
    {
        lockNodeAndExecute( dummyNodeId, () ->
        {
            for ( int i = 0; i < 4; i++ )
            {
                updateNodeViaBolt( dummyNodeId, OTHER_USER, OTHER_USER_PWD );
            }
            for ( int i = 0; i < 1; i++ )
            {
                updateNodeViaHttp( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }
            for ( int i = 0; i < 5; i++ )
            {
                updateNodeViaHttps( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }

            awaitNumberOfAuthenticatedConnectionsToBe( 10 );
        } );
        verifyConnectionCount( BOLT, OTHER_USER, 4 );
        verifyConnectionCount( HTTP, "neo4j", 1 );
        verifyConnectionCount( HTTPS, "neo4j", 5 );
    }

    @Test
    void shouldKillHttpConnection() throws Exception
    {
        testKillingOfConnections( neo4j.httpURI(), HTTP, 4 );
    }

    @Test
    void shouldKillHttpsConnection() throws Exception
    {
        testKillingOfConnections( neo4j.httpsURI(), HTTPS, 2 );
    }

    @Test
    void shouldKillBoltConnection() throws Exception
    {
        testKillingOfConnections( neo4j.boltURI(), BOLT, 3 );
    }

    private void testListingOfUnauthenticatedConnections( int httpCount, int httpsCount, int boltCount ) throws Exception
    {
        for ( int i = 0; i < httpCount; i++ )
        {
            connectSocketTo( neo4j.httpURI() );
        }

        for ( int i = 0; i < httpsCount; i++ )
        {
            connectSocketTo( neo4j.httpsURI() );
        }

        for ( int i = 0; i < boltCount; i++ )
        {
            connectSocketTo( neo4j.boltURI() );
        }

        awaitNumberOfAcceptedConnectionsToBe( httpCount + httpsCount + boltCount );

        verifyConnectionCount( HTTP, null, httpCount );
        verifyConnectionCount( HTTPS, null, httpsCount );
        verifyConnectionCount( BOLT, null, boltCount );
    }

    private void testKillingOfConnections( URI uri, TestConnector connector, int count ) throws Exception
    {
        List<TransportConnection> socketConnections = new ArrayList<>();
        for ( int i = 0; i < count; i++ )
        {
            socketConnections.add( connectSocketTo( uri ) );
        }

        awaitNumberOfAcceptedConnectionsToBe( count );
        verifyConnectionCount( connector, null, count );

        killAcceptedConnectionViaBolt();
        verifyConnectionCount( connector, null, 0 );

        for ( TransportConnection socketConnection : socketConnections )
        {
            assertConnectionBreaks( socketConnection );
        }
    }

    private TransportConnection connectSocketTo( URI uri ) throws IOException
    {
        SocketConnection connection = new SocketConnection();
        connections.add( connection );
        connection.connect( new HostnamePort( uri.getHost(), uri.getPort() ) );
        return connection;
    }

    private void awaitNumberOfAuthenticatedConnectionsToBe( int n ) throws InterruptedException
    {
        assertEventually( "Unexpected number of authenticated connections",
                this::authenticatedConnectionsFromConnectionTracker, hasSize( n ),
                1, MINUTES );
    }

    private void awaitNumberOfAcceptedConnectionsToBe( int n ) throws InterruptedException
    {
        assertEventually( connections -> "Unexpected number of accepted connections: " + connections,
                this::acceptedConnectionsFromConnectionTracker, hasSize( n ),
                1, MINUTES );
    }

    private void verifyConnectionCount( TestConnector connector, String username, int expectedCount ) throws InterruptedException
    {
        verifyConnectionCount( connector, username, expectedCount, false );
    }

    private void verifyAuthenticatedConnectionCount( TestConnector connector, String username, int expectedCount ) throws InterruptedException
    {
        verifyConnectionCount( connector, username, expectedCount, true );
    }

    private void verifyConnectionCount( TestConnector connector, String username, int expectedCount, boolean expectAuthenticated )
            throws InterruptedException
    {
        assertEventually( connections -> "Unexpected number of listed connections: " + connections,
                () -> listMatchingConnection( connector, username, expectAuthenticated ), hasSize( expectedCount ),
                1, MINUTES );
    }

    private List<Map<String,Object>> listMatchingConnection( TestConnector connector, String username, boolean expectAuthenticated )
    {
        List<Map<String,Object>> matchingRecords = new ArrayList<>();
        try ( Transaction transaction = db.beginTx() )
        {
            Result result = db.execute( "CALL dbms.listConnections()" );
            assertEquals( LIST_CONNECTIONS_PROCEDURE_COLUMNS, result.columns() );
            List<Map<String,Object>> records = result.stream().collect( toList() );

            for ( Map<String,Object> record : records )
            {
                String actualConnector = record.get( "connector" ).toString();
                assertNotNull( actualConnector );
                Object actualUsername = record.get( "username" );
                if ( Objects.equals( connector.name, actualConnector ) && Objects.equals( username, actualUsername ) )
                {
                    if ( expectAuthenticated )
                    {
                        assertEquals( connector.userAgent, record.get( "userAgent" ) );
                    }

                    matchingRecords.add( record );
                }

                assertThat( record.get( "connectionId" ).toString(), startsWith( actualConnector ) );
                OffsetDateTime connectTime = ISO_OFFSET_DATE_TIME.parse( record.get( "connectTime" ).toString(), OffsetDateTime::from );
                assertNotNull( connectTime );
                assertThat( record.get( "serverAddress" ), instanceOf( String.class ) );
                assertThat( record.get( "clientAddress" ), instanceOf( String.class ) );
            }
            transaction.commit();
        }
        return matchingRecords;
    }

    private List<TrackedNetworkConnection> authenticatedConnectionsFromConnectionTracker()
    {
        return acceptedConnectionsFromConnectionTracker().stream()
                .filter( connection -> connection.username() != null )
                .collect( toList() );
    }

    private List<TrackedNetworkConnection> acceptedConnectionsFromConnectionTracker()
    {
        NetworkConnectionTracker connectionTracker = db.getDependencyResolver().resolveDependency( NetworkConnectionTracker.class );
        return connectionTracker.activeConnections();
    }

    private void changeDefaultPasswordForUserNeo4j( String newPassword )
    {
        String changePasswordUri = neo4j.httpURI().resolve( "user/neo4j/password" ).toString();
        Response response = withBasicAuth( "neo4j", "neo4j" )
                .POST( changePasswordUri, quotedJson( "{'password':'" + newPassword + "'}" ) );

        assertEquals( 204, response.status() );
    }

    private void createNewUser( String username, String password )
    {
        String uri = txCommitUri( false );

        Response response1 = withBasicAuth( "neo4j", NEO4J_USER_PWD )
                .POST( uri, query( "CALL dbms.security.createUser(\\\"" + username + "\\\", \\\"" + password + "\\\", false)" ) );
        assertEquals( 200, response1.status() );

        Response response2 = withBasicAuth( "neo4j", NEO4J_USER_PWD )
                .POST( uri, query( "CALL dbms.security.addRoleToUser(\\\"admin\\\", \\\"" + username + "\\\")" ) );
        assertEquals( 200, response2.status() );
    }

    private long createDummyNode()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            long id;
            try ( Result result = db.execute( "CREATE (n:Dummy) RETURN id(n) AS i" ) )
            {
                Map<String,Object> record = single( result );
                id = (long) record.get( "i" );
            }
            transaction.commit();
            return id;
        }
    }

    private void lockNodeAndExecute( long id, ThrowingAction<Exception> action ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( id );
            Lock lock = tx.acquireWriteLock( node );
            try
            {
                action.apply();
            }
            finally
            {
                lock.release();
            }
            tx.rollback();
        }
    }

    private Future<Response> updateNodeViaHttp( long id, String username, String password )
    {
        return updateNodeViaHttp( id, false, username, password );
    }

    private Future<Response> updateNodeViaHttps( long id, String username, String password )
    {
        return updateNodeViaHttp( id, true, username, password );
    }

    private Future<Response> updateNodeViaHttp( long id, boolean encrypted, String username, String password )
    {
        String uri = txCommitUri( encrypted );
        String userAgent = encrypted ? HTTPS.userAgent : HTTP.userAgent;

        return executor.submit( () ->
                withBasicAuth( username, password )
                        .withHeaders( HttpHeaders.USER_AGENT, userAgent )
                        .POST( uri, query( "MATCH (n) WHERE id(n) = " + id + " SET n.prop = 42" ) )
        );
    }

    private Future<Void> updateNodeViaBolt( long id, String username, String password )
    {
        return executor.submit( () ->
        {
            connectSocketTo( neo4j.boltURI() )
                    .send( util.defaultAcceptedVersions() )
                    .send( auth( username, password ) )
                    .send( util.defaultRunAutoCommitTx( "MATCH (n) WHERE id(n) = " + id + " SET n.prop = 42" ) );

            return null;
        } );
    }

    private void killAcceptedConnectionViaBolt() throws Exception
    {
        for ( TrackedNetworkConnection connection : acceptedConnectionsFromConnectionTracker() )
        {
            killConnectionViaBolt( connection );
        }
    }

    private void killConnectionViaBolt( TrackedNetworkConnection trackedConnection ) throws Exception
    {
        String id = trackedConnection.id();
        String user = trackedConnection.username();

        TransportConnection connection = connectSocketTo( neo4j.boltURI() );
        try
        {
            connection.send( util.defaultAcceptedVersions() )
                    .send( auth( "neo4j", NEO4J_USER_PWD ) )
                    .send( util.defaultRunAutoCommitTx( "CALL dbms.killConnection('" + id + "')" ) );

            assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
            assertThat( connection, util.eventuallyReceives(
                    msgSuccess(),
                    msgSuccess(),
                    msgRecord( eqRecord( any( Value.class ), equalTo( stringOrNoValue( user ) ), equalTo( stringValue( "Connection found" ) ) ) ),
                    msgSuccess() ) );
        }
        finally
        {
            connection.disconnect();
        }
    }

    private static void assertConnectionBreaks( TransportConnection connection ) throws TimeoutException
    {
        Predicates.await( () -> connectionIsBroken( connection ), 1, MINUTES );
    }

    private static boolean connectionIsBroken( TransportConnection connection )
    {
        try
        {
            connection.send( new byte[]{1} );
            connection.recv( 1 );
            return false;
        }
        catch ( SocketException e )
        {
            return true;
        }
        catch ( IOException e )
        {
            return false;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( e );
        }
    }

    private void terminateAllTransactions()
    {
        KernelTransactions kernelTransactions = db.getDependencyResolver().resolveDependency( KernelTransactions.class );
        kernelTransactions.activeTransactions().forEach( h -> h.markForTermination( Terminated ) );
    }

    private String txCommitUri( boolean encrypted )
    {
        URI baseUri = encrypted ? neo4j.httpsURI() : neo4j.httpURI();
        return baseUri.resolve( "db/neo4j/tx/commit" ).toString();
    }

    private static RawPayload query( String statement )
    {
        return rawPayload( "{\"statements\":[{\"statement\":\"" + statement + "\"}]}" );
    }

    private byte[] auth( String username, String password ) throws IOException
    {
        Map<String,Object> authToken = map( "scheme", "basic", "principal", username, "credentials", password, "user_agent", BOLT.userAgent );
        return util.defaultAuth( authToken );
    }

    enum TestConnector
    {
        HTTP( "http", "http-user-agent" ),
        HTTPS( "https", "https-user-agent" ),
        BOLT( "bolt", "bolt-user-agent" );

        final String name;
        final String userAgent;

        TestConnector( String name, String userAgent )
        {
            this.name = name;
            this.userAgent = userAgent;
        }
    }
}
