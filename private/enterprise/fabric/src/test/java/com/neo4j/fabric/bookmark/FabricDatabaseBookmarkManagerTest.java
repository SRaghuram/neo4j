/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerImpl;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.fabric.TestUtils.createUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class FabricDatabaseBookmarkManagerTest
{
    private final UUID location1Uuid = UUID.randomUUID();
    private final UUID location2Uuid = UUID.randomUUID();
    private final Location.Remote.External location1 = new Location.Remote.External( 1, location1Uuid, createUri( "bolt://somewhere:1001" ), null );
    private final Location.Remote.External location2 = new Location.Remote.External( 2, location2Uuid, createUri( "bolt://somewhere:1002" ), null );

    private final TransactionIdTracker transactionIdTracker = mock( TransactionIdTracker.class );
    private final LocalGraphTransactionIdTracker localGraphTransactionIdTracker =
            new LocalGraphTransactionIdTracker( transactionIdTracker, null, Duration.ofSeconds( 1 ) );
    private final TransactionBookmarkManager bookmarkManager = new TransactionBookmarkManagerImpl( localGraphTransactionIdTracker, false );

    @Test
    void testBasicRemoteBookmarkHandling()
    {
        bookmarkManager.processSubmittedByClient( List.of() );

        bookmarkManager.remoteTransactionCommitted( location1, bookmark( "BB-1" ));
        bookmarkManager.remoteTransactionCommitted( location2, bookmark( "BB-2" ));

        var bookmark = bookmarkManager.constructFinalBookmark();
        var graph1State = getGraphState( bookmark, location1 );
        assertThat( graph1State ).contains( "BB-1" );
        var graph2State = getGraphState( bookmark, location2 );
        assertThat( graph2State ).contains( "BB-2" );
    }

    @Test
    void testSubmittedBookmarkHandling()
    {
        var b1 = bookmark( graphState( location1Uuid, "BB-1", "BB-2" ), graphState( location2Uuid, "BB-3", "BB-4" ) );
        var b2 = bookmark( graphState( location1Uuid, "BB-5" ) );
        bookmarkManager.processSubmittedByClient( List.of( b1, b2 ) );

        assertThat( getBookmarksForGraph( location1 ) ).contains( "BB-1", "BB-2", "BB-5" );
        assertThat( getBookmarksForGraph( location2 ) ).contains( "BB-3", "BB-4" );

        bookmarkManager.remoteTransactionCommitted( location1, bookmark( "BB-6" ));
        bookmarkManager.remoteTransactionCommitted( location2, bookmark("BB-7" ));

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
        var b1 = bookmark( graphState( location1Uuid, "BB-1", "BB-2" ) );
        var b2 = bookmark( graphState( location1Uuid, "BB-3" ) );
        bookmarkManager.processSubmittedByClient( List.of( b1, b2 ) );

        var bookmark = bookmarkManager.constructFinalBookmark();
        var graph1State = getGraphState( bookmark, location1 );
        assertThat( graph1State ).contains( "BB-1", "BB-2", "BB-3" );
    }

    @Test
    void testSystemDbBookmark()
    {
        var b1 = bookmark( graphState( location1Uuid, "BB-1" ) );
        var b2 = new SystemDbBookmark( 1234 );
        bookmarkManager.processSubmittedByClient( List.of( b1, b2 ) );

        verify( transactionIdTracker ).awaitUpToDate( NAMED_SYSTEM_DATABASE_ID, 1234L, Duration.ofSeconds( 1 ) );
        verify( transactionIdTracker, times( 1 ) ).awaitUpToDate( any(), anyLong(), any() );

        assertThat( getBookmarksForGraph( location1 ) ).contains( "BB-1" );
    }

    private List<String> getBookmarksForGraph( Location.Remote graph )
    {
        return bookmarkManager.getBookmarksForRemote( graph ).stream().map( RemoteBookmark::getSerialisedState ).collect( Collectors.toList());
    }

    private List<String> getGraphState( FabricBookmark fabricBookmark, Location location )
    {
        List<FabricBookmark.ExternalGraphState> graphStates = fabricBookmark.getExternalGraphStates().stream()
                .filter( gs -> gs.getGraphUuid().equals( location.getUuid() ) )
                .collect( Collectors.toList());
        assertEquals(1, graphStates.size());
        return graphStates.get( 0 ).getBookmarks().stream()
                .map( RemoteBookmark::getSerialisedState )
                .collect( Collectors.toList());
    }

    private FabricBookmark.ExternalGraphState graphState( UUID uuid, String... bookmarks )
    {
        return new FabricBookmark.ExternalGraphState( uuid, Arrays.stream( bookmarks )
                .map( this::bookmark )
                .collect( Collectors.toList()) );
    }

    private RemoteBookmark bookmark( String state )
    {
        return new RemoteBookmark( state );
    }

    private FabricBookmark bookmark( FabricBookmark.ExternalGraphState... states )
    {
        return new FabricBookmark( List.of(), Arrays.asList( states ) );
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
