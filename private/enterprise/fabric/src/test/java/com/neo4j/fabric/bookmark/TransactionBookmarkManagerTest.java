/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.bolt.FabricBookmarkParser;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerImpl;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class TransactionBookmarkManagerTest
{
    private final UUID external1Uuid = UUID.randomUUID();
    private final UUID external2Uuid = UUID.randomUUID();
    private final UUID internal1Uuid = UUID.randomUUID();
    private final UUID internal2Uuid = UUID.randomUUID();
    private final UUID internal3Uuid = UUID.randomUUID();
    private final UUID local1Uuid = UUID.randomUUID();
    private final UUID local2Uuid = UUID.randomUUID();

    private final Location.Remote.External external1 = new Location.Remote.External( 1, external1Uuid, null, null );
    private final Location.Remote.External external2 = new Location.Remote.External( 2, external2Uuid, null, null );
    private final Location.Remote.Internal internal1 = new Location.Remote.Internal( 3, internal1Uuid, null, null );
    private final Location.Remote.Internal internal2 = new Location.Remote.Internal( 4, internal2Uuid, null, null );
    private final Location.Remote.Internal internal3 = new Location.Remote.Internal( 5, internal3Uuid, null, null );
    private final Location.Local local1 = new Location.Local( 6, local1Uuid, null );
    private final Location.Local local2 = new Location.Local( 7, local2Uuid, null );

    private final LocalGraphTransactionIdTracker transactionIdTracker = mock( LocalGraphTransactionIdTracker.class );
    private final TransactionBookmarkManager bookmarkManager = new TransactionBookmarkManagerImpl( transactionIdTracker, true );

    @Test
    void testBookmarkHandling()
    {
        bookmarkManager.processSubmittedByClient( List.of() );

        bookmarkManager.remoteTransactionCommitted( external1, bookmark( "BB-1" ));
        bookmarkManager.remoteTransactionCommitted( external2, bookmark( "BB-2" ));

        bookmarkManager.remoteTransactionCommitted( internal1, fabricRemoteBookmark( internal1Uuid, 1001L ));
        bookmarkManager.remoteTransactionCommitted( internal2, fabricRemoteBookmark( internal2Uuid, 1002L ));
        bookmarkManager.remoteTransactionCommitted( internal3, fabricRemoteBookmark( external1Uuid, "BB-3") );

        mockTxIdTracker( local1, 1003L );
        mockTxIdTracker( local2, 1004L );

        bookmarkManager.localTransactionCommitted( local1 );
        bookmarkManager.localTransactionCommitted( local2 );

        var fabricBookmark = bookmarkManager.constructFinalBookmark();

        verifyInternal( fabricBookmark, internal1Uuid, 1001L );
        verifyInternal( fabricBookmark, internal2Uuid, 1002L );
        verifyInternal( fabricBookmark, local1Uuid, 1003L );
        verifyInternal( fabricBookmark, local2Uuid, 1004L );

        verifyExternal( fabricBookmark, external1Uuid, "BB-1", "BB-3" );
        verifyExternal( fabricBookmark, external2Uuid, "BB-2" );
    }

    @Test
    void testSubmittedBookmarkHandling()
    {
        var b1 = fabricBookmark( internal1Uuid, 1001L );
        var b2 = fabricBookmark( internal2Uuid, 1002L );
        var b3 = fabricBookmark( internal1Uuid, 1003L );

        var b4 = fabricBookmark( local1Uuid, 1004L );
        var b5 = fabricBookmark( local2Uuid, 1005L );
        var b6 = fabricBookmark( local1Uuid, 1006L );

        var b7 = fabricBookmark( external1Uuid, "BB-1", "BB-2" );
        var b8 = fabricBookmark( external2Uuid, "BB-3" );
        var b9 = fabricBookmark( external1Uuid, "BB-4" );

        bookmarkManager.processSubmittedByClient( List.of( b1, b2, b3, b4, b5, b6, b7, b8, b9 ) );

        var internal1Bookmark = toFabricBookmark( bookmarkManager.getBookmarksForRemote( internal1 ) );

        verifyInternal( internal1Bookmark, internal1Uuid, 1003L );
        verifyInternal( internal1Bookmark, internal2Uuid, 1002L );
        verifyInternal( internal1Bookmark, local1Uuid, 1006L );
        verifyInternal( internal1Bookmark, local2Uuid, 1005L );

        verifyExternal( internal1Bookmark, external1Uuid, "BB-1", "BB-2", "BB-4" );
        verifyExternal( internal1Bookmark, external2Uuid, "BB-3" );

        var external1Bookmarks = bookmarkManager.getBookmarksForRemote( external1 );
        var external1RawBookmarks = external1Bookmarks.stream().map( RemoteBookmark::getSerialisedState ).collect( Collectors.toList() );
        assertThat( external1RawBookmarks ).contains( "BB-1", "BB-2", "BB-4" );

        bookmarkManager.awaitUpToDate( local1 );
        verify( transactionIdTracker).awaitGraphUpToDate( local1, 1006L );

        mockTxIdTracker( local2, 1007L );
        bookmarkManager.localTransactionCommitted( local2 );
        bookmarkManager.remoteTransactionCommitted( internal2, fabricRemoteBookmark( internal1Uuid, 1008L ) );
        bookmarkManager.remoteTransactionCommitted( external2, new RemoteBookmark( "BB-5" ) );

        var fabricBookmark = bookmarkManager.constructFinalBookmark();

        verifyInternal( fabricBookmark, internal1Uuid, 1008L );
        verifyInternal( fabricBookmark, internal2Uuid, 1002L );
        verifyInternal( fabricBookmark, local1Uuid, 1006L );
        verifyInternal( fabricBookmark, local2Uuid, 1007L );

        verifyExternal( fabricBookmark, external1Uuid, "BB-1", "BB-2", "BB-4" );
        verifyExternal( fabricBookmark, external2Uuid, "BB-5" );
    }

    @Test
    void testSystemDbBookmark()
    {
        var b1 = fabricBookmark( internal1Uuid, 1001L );
        var b2 = fabricBookmark( NAMED_SYSTEM_DATABASE_ID.databaseId().uuid(), 1002L );

        var transactionIdTracker = mock( TransactionIdTracker.class );
        var localGraphTransactionIdTracker = new LocalGraphTransactionIdTracker( transactionIdTracker, null, Duration.ofSeconds( 1 ) );
        var bookmarkManager = new TransactionBookmarkManagerImpl( localGraphTransactionIdTracker, true );
        bookmarkManager.processSubmittedByClient( List.of( b1, b2 ) );

        verify( transactionIdTracker ).awaitUpToDate( NAMED_SYSTEM_DATABASE_ID, 1002L, Duration.ofSeconds( 1 ) );
        verify( transactionIdTracker, times( 1 ) ).awaitUpToDate( any(), anyLong(), any() );
    }

    private FabricBookmark toFabricBookmark( List<RemoteBookmark> remoteBookmarks )
    {
        assertEquals( 1, remoteBookmarks.size() );
        var remoteBookmark = remoteBookmarks.get( 0 );
        var fabricBookmarkParser = new FabricBookmarkParser();
        return fabricBookmarkParser.parse( remoteBookmark.getSerialisedState() );
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

    private void verifyExternal( FabricBookmark fabricBookmark, UUID uuid, String... expectedExternalBookmarks )
    {
        var externalBookmarks = fabricBookmark.getExternalGraphStates().stream()
                .filter( gs -> gs.getGraphUuid().equals( uuid ) )
                .flatMap( gs -> gs.getBookmarks().stream() )
                .map( RemoteBookmark::getSerialisedState )
                .collect( Collectors.toList() );
        assertThat(externalBookmarks).contains( expectedExternalBookmarks );
    }

    private RemoteBookmark bookmark( String state )
    {
        return new RemoteBookmark( state );
    }

    private FabricBookmark fabricBookmark( UUID uuid, long txId )
    {
        var internalGraphState = new FabricBookmark.InternalGraphState( uuid, txId );
        return new FabricBookmark( List.of( internalGraphState ), List.of() );
    }

    private RemoteBookmark fabricRemoteBookmark( UUID uuid, long txId )
    {
        var fabricBookmark = fabricBookmark( uuid, txId );
        return new RemoteBookmark( fabricBookmark.serialize() );
    }

    private FabricBookmark fabricBookmark( UUID uuid, String... externalBookmarks )
    {
        var remoteBookmarks = Arrays.stream( externalBookmarks ).map( RemoteBookmark::new ).collect( Collectors.toList() );
        var externalBookmarkState = new FabricBookmark.ExternalGraphState( uuid, remoteBookmarks );
        return new FabricBookmark( List.of(), List.of( externalBookmarkState ) );
    }

    private RemoteBookmark fabricRemoteBookmark( UUID uuid, String... externalBookmarks )
    {
        var fabricBookmark = fabricBookmark( uuid, externalBookmarks );
        return new RemoteBookmark( fabricBookmark.serialize() );
    }

    private void mockTxIdTracker( Location.Local location, long txId )
    {
        when( transactionIdTracker.getTransactionId( location ) ).thenReturn( txId );
    }
}
