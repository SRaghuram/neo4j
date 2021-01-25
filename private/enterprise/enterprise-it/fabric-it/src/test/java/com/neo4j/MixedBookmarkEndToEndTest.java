/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class MixedBookmarkEndToEndTest
{
    private static Driver clientDriver;
    private static TestFabric testFabric;
    private static TestBoltServer remote1;
    private static TestBoltServer remote2;

    @BeforeAll
    static void beforeAll()
    {
        remote1 = new TestBoltServer();
        remote2 = new TestBoltServer();

        List.<Runnable>of(
                () -> remote1.start(),
                () -> remote2.start()
        ).parallelStream().forEach( Runnable::run );

        var additionalProperties = Map.of(
                "fabric.graph.0.uri", remote1.getBoltUri().toString(),
                "fabric.graph.1.uri", remote2.getBoltUri().toString()
        );

        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withAdditionalSettings( additionalProperties )
                .build();

        clientDriver = testFabric.routingClientDriver();
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> testFabric.close(),
                () -> remote1.stop(),
                () -> remote2.stop()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testBasicLifecycle()
    {
        ArgumentCaptor<List<Bookmark>> submittedBookmarks = mockRemote( remote1, List.of( 1111L, 2222L ) );

        var b1 = run( "USE mega.graph(0) RETURN 1", List.of() );

        var b2 = run( "USE mega.graph(0) RETURN 1", List.of( b1 ) );

        verifyBookmarks( submittedBookmarks, List.of( 1111L ) );

        run( "USE mega.graph(0) RETURN 1", List.of( b2 ) );

        verifyBookmarks( submittedBookmarks, List.of( 2222L ) );
    }

    @Test
    void testBasicLifecycleWithWrite()
    {
        ArgumentCaptor<List<Bookmark>> submittedBookmarks = mockRemote( remote1, List.of( 1111L ) );

        var b = run( "USE mega.graph(0) CREATE() RETURN 1", List.of() );

        run( "USE mega.graph(0) RETURN 1", List.of( b ) );

        verifyBookmarks( submittedBookmarks, List.of( 1111L ) );
    }

    @Test
    void testBookmarksToMultipleRemotes()
    {
        ArgumentCaptor<List<Bookmark>> submittedBookmarksOnRemote1 = mockRemote( remote1, List.of( 1111L, 2222L, 3333L ) );
        ArgumentCaptor<List<Bookmark>> submittedBookmarksOnRemote2 = mockRemote( remote2, List.of( 4444L, 5555L ) );

        var b1 = run( "USE mega.graph(0) RETURN 1", List.of() );
        var b2 = run( "USE mega.graph(1) RETURN 1", List.of() );

        var query = String.join( "\n",
                "UNWIND [0, 1] AS gid",
                "CALL {",
                "  USE mega.graph(gid)",
                "  RETURN 1",
                "}",
                "RETURN 2"
        );

        var b3 = run( query, List.of( b1, b2 ) );
        verifyBookmarks( submittedBookmarksOnRemote1, List.of( 1111L ) );
        verifyBookmarks( submittedBookmarksOnRemote2, List.of( 4444L ) );

        var b4 = run( "USE mega.graph(0) RETURN 1", List.of( b3 ) );
        verifyBookmarks( submittedBookmarksOnRemote1, List.of( 2222L ) );

        run( query, List.of( b4 ) );
        verifyBookmarks( submittedBookmarksOnRemote1, List.of( 3333L ) );
        verifyBookmarks( submittedBookmarksOnRemote2, List.of( 5555L ) );
    }

    @Test
    void testRemoteBookmarksComposition()
    {
        ArgumentCaptor<List<Bookmark>> submittedBookmarks = mockRemote( remote1, List.of( 1111L, 2222L, 3333L, 4444L, 5555L ) );

        var b1 = run( "USE mega.graph(0) RETURN 1", List.of() );
        var b2 = run( "USE mega.graph(0) RETURN 1", List.of() );
        var b3 = run( "USE mega.graph(0) RETURN 1", List.of() );

        var b4 = run( "USE mega.graph(0) RETURN 1", List.of( b1, b3, b2 ) );
        // all three (1111, 3333, 2222) should be submitted to the remote, the remote returns only the one with the highest TX ID from the bookmark parser
        verifyBookmarks( submittedBookmarks, List.of( 3333L ) );

        run( "USE mega.graph(0) RETURN 1", List.of( b4 ) );
        verifyBookmarks( submittedBookmarks, List.of( 4444L ) );
    }

    @Test
    void testEmptyBookmark()
    {
        ArgumentCaptor<List<Bookmark>> submittedBookmarks = mockRemote( remote1, List.of( 1111L, 2222L ) );

        var b1 = run( "RETURN 1", List.of() );

        var b2 = run( "USE mega.graph(0) RETURN 1", List.of( b1 ) );

        verifyBookmarks( submittedBookmarks, List.of() );

        run( "USE mega.graph(0) RETURN 1", List.of( b2 ) );

        verifyBookmarks( submittedBookmarks, List.of( 1111L ) );
    }

    private org.neo4j.driver.Bookmark run( String statement, List<org.neo4j.driver.Bookmark> bookmarks )
    {
        try ( var session = clientDriver.session( SessionConfig.builder().withBookmarks( bookmarks ).withDatabase( "mega" ).build() ) )
        {
            session.run( statement ).consume();
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
        assertThat( txIds ).contains( expectedTxIds.toArray( Long[]::new ) );
    }
}
