/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import com.neo4j.fabric.driver.RemoteBookmark;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.runtime.Bookmark;

public class FabricBookmarkParser implements CustomBookmarkFormatParser
{
    @Override
    public boolean isCustomBookmark( String string )
    {
        return string.startsWith( FabricBookmark.PREFIX );
    }

    @Override
    public List<Bookmark> parse( List<String> customBookmarks )
    {
        return customBookmarks.stream().map( this::parse ).collect( Collectors.toList());
    }

    private Bookmark parse( String bookmarkString )
    {
        if ( !isCustomBookmark( bookmarkString ) )
        {
            throw new IllegalArgumentException( String.format( "'%s' is not a valid Fabric bookmark", bookmarkString ) );
        }

        var content = bookmarkString.substring( FabricBookmark.PREFIX.length() );

        if ( content.isEmpty() )
        {
            return new FabricBookmark( List.of() );
        }

        var graphParts = content.split( "-" );

        var remoteStates = Arrays.stream( graphParts )
                .map( rawGraphState -> parseGraphState( bookmarkString, rawGraphState ) )
                .collect( Collectors.toList() );

        return new FabricBookmark( remoteStates );
    }

    private FabricBookmark.GraphState parseGraphState( String bookmarkString, String rawGraphState )
    {
        var parts = rawGraphState.split( ":" );
        if ( parts.length != 2 )
        {
            throw new IllegalArgumentException( String.format( "Bookmark '%s' not valid", bookmarkString ) );
        }

        long graphId;
        try
        {
            graphId = Long.parseLong( parts[0] );
        }
        catch ( NumberFormatException e )
        {
            throw new IllegalArgumentException( String.format( "Could not parse graph ID in '%s'", bookmarkString ), e );
        }

        var remoteBookmarks = Arrays.stream( parts[1].split( "," ) )
                .map( this::decodeRemoteBookmark )
                .collect( Collectors.toList() );
        return new FabricBookmark.GraphState( graphId, remoteBookmarks );
    }

    private RemoteBookmark decodeRemoteBookmark( String encodedBookmark )
    {
        var decodedBookmarkState = Arrays.stream( encodedBookmark.split( "\\|" ) )
                .map( bookmarkPart -> Base64.getDecoder().decode( bookmarkPart ) )
                .map( decodedPart -> new String( decodedPart, StandardCharsets.UTF_8 ) )
                .collect( Collectors.toSet());
        return new RemoteBookmark( decodedBookmarkState );
    }
}
