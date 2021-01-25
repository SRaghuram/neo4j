/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.bolt.FabricBookmarkParser;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.kernel.api.exceptions.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class BookmarkSerializationAndParsingTest
{
    private final FabricBookmarkParser parser = new FabricBookmarkParser();

    private final BookmarkBuilder bookmarkBuilder = new BookmarkBuilder();
    private final UUID uuid1 = UUID.randomUUID();
    private final UUID uuid2 = UUID.randomUUID();
    private final UUID uuid3 = UUID.randomUUID();
    private final UUID uuid4 = UUID.randomUUID();

    @Test
    void testBasicBookmarkSerialization()
    {
        bookmarkBuilder.addExternal( uuid1, "b1", "b1-p2" );
        bookmarkBuilder.addExternal( uuid2, "b2" );
        bookmarkBuilder.addInternal( uuid3, 1001L );
        bookmarkBuilder.addInternal( uuid4, 1002L );

        var fabricBookmark = bookmarkBuilder.build();
        var parsedBookmark = parser.parse( fabricBookmark.serialize() );

        verifyExternal( parsedBookmark, uuid1, "b1", "b1-p2" );
        verifyExternal( parsedBookmark, uuid2, "b2" );
        verifyInternal( parsedBookmark, uuid3, 1001L );
        verifyInternal( parsedBookmark, uuid4, 1002L );
    }

    @Test
    void testEmptyBookmark()
    {
        bookmarkBuilder.build();

        var fabricBookmark = bookmarkBuilder.build();
        var parsedBookmark = parser.parse( fabricBookmark.serialize() );

        Assertions.assertEquals( 0, parsedBookmark.getInternalGraphStates().size() );
        Assertions.assertEquals( 0, parsedBookmark.getExternalGraphStates().size() );
    }

    @Test
    void testFabricBookmarkRecognition()
    {
        assertFalse( parser.isCustomBookmark( "abcd" ) );
        assertTrue( parser.isCustomBookmark( "FB:abcd" ) );
    }

    @Test
    void testInvalidBookmark()
    {
        try
        {
            parser.parse( List.of( "FB:abcd" ) );
            fail();
        }
        catch ( FabricException e )
        {
            assertEquals( Status.Transaction.InvalidBookmark, e.status() );
            assertThat( e.getMessage()).contains( "Failed to deserialize bookmark" );
        }
        catch ( Exception e )
        {
            fail();
        }
    }

    private void verifyExternal( FabricBookmark fabricBookmark, UUID uuid, String... expectedRemoteBookmark )
    {
        var parsedExternalBookmarks = fabricBookmark.getExternalGraphStates().stream()
                .filter( egs -> egs.getGraphUuid().equals( uuid ) )
                .flatMap( egs -> egs.getBookmarks().stream() )
                .map( RemoteBookmark::getSerialisedState )
                .collect( Collectors.toList());
        assertThat(parsedExternalBookmarks).contains( expectedRemoteBookmark );
    }

    private void verifyInternal( FabricBookmark fabricBookmark, UUID uuid, long expectedTransactionId )
    {
        var txId = fabricBookmark.getInternalGraphStates().stream()
                .filter( igs -> igs.getGraphUuid().equals( uuid ) )
                .map( FabricBookmark.InternalGraphState::getTransactionId )
                .findAny()
                .get();

        assertEquals( expectedTransactionId, txId );
    }

    private static class BookmarkBuilder
    {
        final List<FabricBookmark.InternalGraphState> internalGraphStates = new ArrayList<>();
        final List<FabricBookmark.ExternalGraphState> externalGraphStates = new ArrayList<>();

        void addExternal( UUID uuid, String... remoteBookmark )
        {
            var rbs = Arrays.stream( remoteBookmark ).map( RemoteBookmark::new ).collect(Collectors.toList());
            externalGraphStates.add( new FabricBookmark.ExternalGraphState( uuid, rbs ) );
        }

        void addInternal( UUID uuid, long transactionId )
        {
            internalGraphStates.add( new FabricBookmark.InternalGraphState( uuid, transactionId ) );
        }

        FabricBookmark build()
        {
            return new FabricBookmark( internalGraphStates, externalGraphStates );
        }
    }
}
