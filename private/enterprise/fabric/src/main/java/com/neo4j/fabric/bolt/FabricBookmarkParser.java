/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import com.neo4j.fabric.bookmark.BookmarkStateSerializer;

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

    public FabricBookmark parse( String bookmarkString )
    {
        if ( !isCustomBookmark( bookmarkString ) )
        {
            throw new IllegalArgumentException( String.format( "'%s' is not a valid Fabric bookmark", bookmarkString ) );
        }

        var content = bookmarkString.substring( FabricBookmark.PREFIX.length() );

        if ( content.isEmpty() )
        {
            return new FabricBookmark( List.of(), List.of() );
        }

        return BookmarkStateSerializer.deserialize( content );
    }
}
