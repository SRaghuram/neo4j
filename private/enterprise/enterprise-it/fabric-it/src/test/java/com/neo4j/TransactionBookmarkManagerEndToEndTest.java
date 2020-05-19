/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.bolt.FabricBookmarkParser;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerImpl;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

@ExtendWith( FabricEverywhereExtension.class )
class TransactionBookmarkManagerEndToEndTest
{
    private static Driver clientDriver;
    private static TestServer testServer;
    private static TestBoltServer remote1;

    private static LocalGraphTransactionIdTracker localGraphTransactionIdTracker = mock(LocalGraphTransactionIdTracker.class);
    private static TransactionBookmarkManagerImpl bookmarkManager = new TransactionBookmarkManagerImpl( localGraphTransactionIdTracker, true );
    private static TransactionBookmarkManagerFactory transactionBookmarkManagerFactory = mock(TransactionBookmarkManagerFactory.class);

    @BeforeAll
    static void beforeAll()
    {
        remote1 = new TestBoltServer();
        remote1.start();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", remote1.getBoltUri().toString(),
                "fabric.graph.0.name", "remote1",
                "fabric.graph.1.uri", "bolt://somewhere:1234",
                "fabric.graph.1.name", "remote2",
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                .setRaw( configProperties )
                .build();
        testServer = new TestServer( config );
        testServer.addMocks( transactionBookmarkManagerFactory );
        testServer.start();

        when( transactionBookmarkManagerFactory.createTransactionBookmarkManager( any() ) ).thenReturn( bookmarkManager );

        clientDriver = GraphDatabase.driver(
                testServer.getBoltDirectUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        createDatabase( "local1" );
        createDatabase( "local2" );
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> testServer.stop(),
                () -> clientDriver.close(),
                () -> remote1.stop()
        ).parallelStream().forEach( Runnable::run );
    }

    @BeforeEach
    void beforeEach()
    {
        reset( localGraphTransactionIdTracker );
    }

    @Test
    void testBookmarkHandling()
    {
        var submittedBookmark = createDriverBookmark( 1001L, 1002L, 1003L, 1004L );
        when( localGraphTransactionIdTracker.getTransactionId( any() ) ).thenReturn( 2001L );

        ArgumentCaptor<List<Bookmark>> submittedBookmarks = mockRemote( remote1, List.of(3001L) );

        var receivedBookmark = runInSession( submittedBookmark, session ->
        {
            try ( var tx = session.beginTransaction() )
            {
                tx.run( "USE local1 RETURN 1" ).consume();
                tx.run( "USE mega.remote1 RETURN 1" ).consume();

                tx.commit();
            }
        } );

        verify( localGraphTransactionIdTracker ).awaitGraphUpToDate( any(), eq( 1001L ) );
        verifyBookmarks( submittedBookmarks, List.of( 1003L ) );

        var fabricBookmark = toFabricBookmark( receivedBookmark );
        verifyInternal( fabricBookmark, getDatabaseUuid( "local1" ), 2001L );
        verifyInternal( fabricBookmark, getDatabaseUuid( "local2" ), 1002L );

        verifyExternal( fabricBookmark, new UUID( 0, 0 ), 3001L );
        verifyExternal( fabricBookmark, new UUID( 1, 0 ), 1004L );
    }

    private FabricBookmark toFabricBookmark( org.neo4j.driver.Bookmark driverBookmark )
    {
        var serializedBookmark = driverBookmark.values().stream().findAny().get();
        var fabricBookmarkParser = new FabricBookmarkParser();
        return fabricBookmarkParser.parse( serializedBookmark );
    }

    private UUID getDatabaseUuid( String name )
    {
        var fabricDatabaseManager = testServer.getDependencies().resolveDependency( FabricDatabaseManager.class );
        var namedDatabaseId = fabricDatabaseManager.databaseIdRepository().getByName( name ).get();
        return namedDatabaseId.databaseId().uuid();
    }

    private static void createDatabase( String name )
    {
        try ( var session = clientDriver.session( SessionConfig.builder().withDatabase( "system" ).build() ) )
        {
            session.run( "CREATE DATABASE " + name ).consume();
        }
    }

    private org.neo4j.driver.Bookmark createDriverBookmark( long local1TxId, long local2TxId, long remote1TxId, long remote2TxId )
    {
        var local1Uuid = getDatabaseUuid( "local1" );
        var local2Uuid = getDatabaseUuid( "local2" );

        var remote1Uuid = new UUID( 0, 0 );
        var remote2Uuid = new UUID( 1, 0 );

        var internalGraphState1 = new FabricBookmark.InternalGraphState( local1Uuid, local1TxId );
        var internalGraphState2 = new FabricBookmark.InternalGraphState( local2Uuid, local2TxId );

        var b1 = new BookmarkWithDatabaseId( remote1TxId, NAMED_SYSTEM_DATABASE_ID );
        var b2 = new BookmarkWithDatabaseId( remote2TxId, NAMED_SYSTEM_DATABASE_ID );
        var externalGraphState1 = new FabricBookmark.ExternalGraphState( remote1Uuid, List.of( new RemoteBookmark( b1.toString() ) ) );
        var externalGraphState2 = new FabricBookmark.ExternalGraphState( remote2Uuid, List.of( new RemoteBookmark( b2.toString() ) ) );

        var fabricBookmark = new FabricBookmark( List.of( internalGraphState1, internalGraphState2 ), List.of( externalGraphState1, externalGraphState2 ) );
        return org.neo4j.driver.Bookmark.from( Set.of( fabricBookmark.serialize() ) );
    }

    private org.neo4j.driver.Bookmark runInSession( org.neo4j.driver.Bookmark bookmark, Consumer<Session> function )
    {
        try ( var session = clientDriver.session( SessionConfig.builder().withBookmarks( bookmark ).build() ) )
        {
            function.accept( session );
            return session.lastBookmark();
        }
    }

    private ArgumentCaptor<List<Bookmark>> mockRemote( TestBoltServer remote, List<Long> txIds )
    {
        ArgumentCaptor<List<Bookmark>> receivedBookmarks = ArgumentCaptor.forClass( List.class );
        try
        {
            reset(remote.boltGraphDatabaseManagementService);
            var database = mock( BoltGraphDatabaseServiceSPI.class );
            when(remote.boltGraphDatabaseManagementService.database( any() )).thenReturn( database );
            var tx = mock( BoltTransaction.class );
            when(database.beginTransaction( any(), any(), any(), receivedBookmarks.capture(), any(), any(), any(), any() )).thenReturn( tx );
            when( database.getNamedDatabaseId() ).thenReturn( NAMED_SYSTEM_DATABASE_ID );

            var bookmarkMetadata = IntStream.range( 1, txIds.size() )
                    .mapToObj( txIds::get )
                    .map( id -> new BookmarkMetadata( id, NAMED_SYSTEM_DATABASE_ID ) )
                    .toArray(BookmarkMetadata[]::new);
            when( tx.getBookmarkMetadata() ).thenReturn( new BookmarkMetadata( txIds.get( 0 ), NAMED_SYSTEM_DATABASE_ID ), bookmarkMetadata );

            when( tx.executeQuery( any(), any(), anyBoolean(), any() ) ).thenAnswer( invocationOnMock ->
            {
                var querySubscriber = invocationOnMock.getArgument( 3, QuerySubscriber.class );

                querySubscriber.onResultCompleted( QueryStatistics.EMPTY );

                return new BoltQueryExecution()
                {

                    @Override
                    public QueryExecution getQueryExecution()
                    {
                        return new QueryExecution()
                        {
                            @Override
                            public QueryExecutionType executionType()
                            {
                                return QueryExecutionType.query( QueryExecutionType.QueryType.READ_WRITE );
                            }

                            @Override
                            public ExecutionPlanDescription executionPlanDescription()
                            {
                                return null;
                            }

                            @Override
                            public Iterable<Notification> getNotifications()
                            {
                                return List.of();
                            }

                            @Override
                            public String[] fieldNames()
                            {
                                return new String[0];
                            }

                            @Override
                            public boolean isVisitable()
                            {
                                return false;
                            }

                            @Override
                            public <VisitationException extends Exception> QueryStatistics accept( Result.ResultVisitor<VisitationException> visitor )
                                    throws VisitationException
                            {
                                return null;
                            }

                            @Override
                            public void request( long numberOfRecords )
                            {

                            }

                            @Override
                            public void cancel()
                            {

                            }

                            @Override
                            public boolean await()
                            {
                                return false;
                            }
                        };
                    }

                    @Override
                    public void close()
                    {

                    }

                    @Override
                    public void terminate()
                    {

                    }
                };
            } );
        }
        catch ( Exception e )
        {
            throw new IllegalArgumentException( e );
        }

        return receivedBookmarks;
    }

    private void verifyBookmarks( ArgumentCaptor<List<Bookmark>> submittedBookmarks, List<Long> expectedTxIds )
    {
        var txIds = submittedBookmarks.getValue().stream().map( Bookmark::txId ).collect( Collectors.toList() );
        Assertions.assertThat( txIds ).contains( expectedTxIds.toArray( Long[]::new ) );
    }

    private void verifyInternal( FabricBookmark fabricBookmark, UUID uuid, long expectedTxId )
    {
        var txId = fabricBookmark.getInternalGraphStates().stream()
                .filter( gs -> gs.getGraphUuid().equals( uuid ) )
                .map( FabricBookmark.InternalGraphState::getTransactionId )
                .findAny()
                .get();

        assertEquals(expectedTxId, txId);
    }

    private void verifyExternal( FabricBookmark fabricBookmark, UUID uuid, long expectedTxId )
    {
        var externalBookmarks = fabricBookmark.getExternalGraphStates().stream()
                .filter( gs -> gs.getGraphUuid().equals( uuid ) )
                .flatMap( gs -> gs.getBookmarks().stream() )
                .map( RemoteBookmark::getSerialisedState )
                .collect( Collectors.toList() );
        var expectedBookmark = new BookmarkWithDatabaseId( expectedTxId, NAMED_SYSTEM_DATABASE_ID ).toString();
        assertThat( externalBookmarks ).contains( expectedBookmark );
    }
}
