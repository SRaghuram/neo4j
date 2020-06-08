/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.neo4j.driver.Bookmark;
import org.neo4j.fabric.bookmark.RemoteBookmark;

public class Utils
{
    static RemoteBookmark convertBookmark( Bookmark bookmark )
    {
        if ( bookmark == null )
        {
            return null;
        }

        // Even though the internal state of Driver's bookmark is a set,
        // currently the set size in a bookmark received from a server is always 1
        // assuming that will simplify bookmark handling a lot
        // if this is changed by the driver, it should fail our tests
        if ( bookmark.values().size() != 1 )
        {
            throw new IllegalArgumentException( "Unexpected bookmark format received from a remote" );
        }

        String serialisedBookmark = bookmark.values().stream().findAny().get();
        return new RemoteBookmark( serialisedBookmark );
    }
}
