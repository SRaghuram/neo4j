/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

import com.neo4j.fabric.bolt.FabricBookmark;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.driver.RemoteBookmark;
import com.neo4j.fabric.executor.Location;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.fabric.TestUtils.createUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class TransactionBookmarkManagerTest
{
    private final Location.Remote location1 = new Location.Remote( 1, createUri( "bolt://somewhere:1001" ), null );
    private final Location.Remote location2 = new Location.Remote( 2, createUri( "bolt://somewhere:1002" ), null );

    private final FabricConfig fabricConfig = mock(FabricConfig.class);
    private final TransactionIdTracker transactionIdTracker = mock(TransactionIdTracker.class);
    private final TransactionBookmarkManager bookmarkManager = new TransactionBookmarkManager( fabricConfig, transactionIdTracker, Duration.ofSeconds( 123 ) );

    @BeforeEach
    void beforeEach()
    {
        var graph1 = new FabricConfig.Graph( 1, FabricConfig.RemoteUri.create( "bolt://somewhere:1001" ), null, null, emptyDriverConfig() );
        var graph2 = new FabricConfig.Graph( 2, FabricConfig.RemoteUri.create( "bolt://somewhere:1002" ), null, null, emptyDriverConfig() );

        var database = mock( FabricConfig.Database.class );
        when( database.getGraphs() ).thenReturn( Set.of( graph1, graph2 ) );
        when( fabricConfig.getDatabase() ).thenReturn( database );
    }

    @Test
    void testBasicRemoteBookmarkHandling()
    {
        bookmarkManager.recordBookmarkReceivedFromGraph( location1, bookmark( "BB-1" ));
        bookmarkManager.recordBookmarkReceivedFromGraph( location2, bookmark( "BB-2" ));
        bookmarkManager.recordBookmarkReceivedFromGraph( location1, bookmark( "BB-3" ));

        var bookmark = bookmarkManager.constructFinalBookmark();
        var graph1State = getGraphState( bookmark, location1 );
        assertThat( graph1State ).contains( "BB-3" );
        var graph2State = getGraphState( bookmark, location2 );
        assertThat( graph2State ).contains( "BB-2" );
    }

    @Test
    void testSubmittedBookmarkHandling()
    {
        var b1 = bookmark( graphState( 1, "BB-1", "BB-2" ), graphState( 2, "BB-3", "BB-4" ) );
        var b2 = bookmark( graphState( 1, "BB-5" ) );
        bookmarkManager.processSubmittedByClient( List.of( b1, b2 ) );

        assertThat( getBookmarksForGraph( location1 ) ).contains( "BB-1", "BB-2", "BB-5" );
        assertThat( getBookmarksForGraph( location2 ) ).contains( "BB-3", "BB-4" );

        bookmarkManager.recordBookmarkReceivedFromGraph( location1, bookmark( "BB-6" ));
        bookmarkManager.recordBookmarkReceivedFromGraph( location2, bookmark("BB-7" ));

        assertThat( getBookmarksForGraph( location1 ) ).contains( "BB-1", "BB-2", "BB-5" );
        assertThat( getBookmarksForGraph( location2 ) ).contains( "BB-3", "BB-4" );

        var bookmark = bookmarkManager.constructFinalBookmark();
        var graph1State = getGraphState( bookmark, location1 );
        assertThat( graph1State ).contains( "BB-6" );
        var graph2State = getGraphState( bookmark, location2 );
        assertThat( graph2State ).contains( "BB-7" );
    }

    @Test
    void testBookmarkMerging()
    {
        var b1 = bookmark( graphState( 1, "BB-1", "BB-2" ) );
        var b2 = bookmark( graphState( 1, "BB-3" ) );
        bookmarkManager.processSubmittedByClient( List.of( b1, b2 ) );

        var bookmark = bookmarkManager.constructFinalBookmark();
        var graph1State = getGraphState( bookmark, location1 );
        assertThat( graph1State ).contains( "BB-1", "BB-2", "BB-3" );
    }

    @Test
    void testSystemDbBookmark()
    {
        var b1 = bookmark( graphState( 1, "BB-1" ) );
        var b2 = new SystemDbBookmark( 1234 );
        bookmarkManager.processSubmittedByClient( List.of( b1, b2 ) );

        verify( transactionIdTracker ).awaitUpToDate( NAMED_SYSTEM_DATABASE_ID, 1234, Duration.ofSeconds( 123 ) );

        assertThat( getBookmarksForGraph( location1 ) ).contains( "BB-1" );
    }

    private List<String> getBookmarksForGraph( Location.Remote graph )
    {
        return bookmarkManager.getBookmarksForGraph( graph ).stream().flatMap( rb -> rb.getSerialisedState().stream() ).collect( Collectors.toList());
    }

    private static FabricConfig.GraphDriverConfig emptyDriverConfig()
    {
        return new FabricConfig.GraphDriverConfig( null, null, null, null, null, null, null, null, false );
    }

    private List<String> getGraphState( FabricBookmark fabricBookmark, Location graph )
    {
        List<FabricBookmark.GraphState> graphStates = fabricBookmark.getGraphStates().stream()
                .filter( gs -> gs.getRemoteGraphId() == graph.getId() )
                .collect( Collectors.toList());
        assertEquals(1, graphStates.size());
        return graphStates.get( 0 ).getBookmarks().stream()
                .flatMap( b -> b.getSerialisedState().stream() )
                .collect( Collectors.toList());
    }

    private FabricBookmark.GraphState graphState( long graphId, String... bookmarks )
    {
        return new FabricBookmark.GraphState( graphId, Arrays.stream( bookmarks ).map( this::bookmark ).collect( Collectors.toList()) );
    }

    private RemoteBookmark bookmark( String state )
    {
        return new RemoteBookmark( Set.of(state) );
    }

    private FabricBookmark bookmark( FabricBookmark.GraphState... states )
    {
        return new FabricBookmark( Arrays.asList( states ) );
    }

    private static class SystemDbBookmark implements Bookmark
    {
        private final long txId;

        SystemDbBookmark( long txId )
        {
            this.txId = txId;
        }

        @Override
        public long txId()
        {
            return txId;
        }

        @Override
        public NamedDatabaseId databaseId()
        {
            return NAMED_SYSTEM_DATABASE_ID;
        }

        @Override
        public void attachTo( BoltResponseHandler state )
        {

        }
    }
}
