/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import java.util.List;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.internal.helpers.collection.Iterables;

// TODO: this should be just a temporary hack until Driver's team provides serialization and deserialization logic for bookmarks
public final class DriverBookmarkFormat
{
    private DriverBookmarkFormat()
    {

    }

    public static Bookmark parse( String serializedBookmark )
    {
        return InternalBookmark.parse( serializedBookmark );
    }

    public static String serialize( Bookmark bookmark )
    {
        var internalBookmark = (InternalBookmark) bookmark;
        List<String> bookmarks = Iterables.asList( internalBookmark.values() );

        switch ( bookmarks.size() )
        {
        case 0:
            return null;
        case 1:
            return bookmarks.get( 0 );
        default:
            throw new IllegalStateException( "Unexpected bookmark format: " + bookmark );
        }
    }
}
