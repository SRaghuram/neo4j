/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class BookmarkSerializationAndParsingTest
{
    private final FabricBookmarkParser parser = new FabricBookmarkParser();

    @Test
    void testBasicBookmarkSerialization()
    {
        var b1 = bookmark( remote( 1, "b1" ) );
        var b2 = bookmark( remote( 2, "b2" ), remote( 3, "b3", "b4" ) );

        doTest( b1, b2 );
    }

    @Test
    void testNoRemoteBookmark()
    {
        doTest( bookmark() );
    }

    @Test
    void testRemoteBookmarkWithSpecialCharacters()
    {
        doTest( bookmark( remote( 1, "-,:" ) ) );
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
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Bookmark 'FB:abcd' not valid", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail();
        }
    }

    @Test
    void testInvalidBookmark2()
    {
        try
        {
            parser.parse( List.of( "FB:abdc:something" ) );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Could not parse graph ID in 'FB:abdc:something'", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail();
        }
    }

    private void doTest( FabricBookmark... fabricBookmark )
    {
        var serializedBookmarks = Arrays.stream( fabricBookmark )
                .map( FabricBookmark::serialize )
                .collect( Collectors.toList() );
        var parsedBookmarks = parser.parse( serializedBookmarks );
        assertThat( parsedBookmarks, containsInAnyOrder( fabricBookmark ) );
    }

    private FabricBookmark bookmark( FabricBookmark.GraphState... graphStates )
    {
        return new FabricBookmark( Arrays.asList( graphStates ) );
    }

    private FabricBookmark.GraphState remote( long graphId, String... bookmarks )
    {
        return new FabricBookmark.GraphState( graphId, Arrays.asList( bookmarks ) );
    }
}
