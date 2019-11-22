/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import java.util.Set;

import org.neo4j.driver.Bookmark;

public class Utils
{
    static RemoteBookmark convertBookmark( Bookmark bookmark )
    {
        Set<String> serialisedBookmark = bookmark == null ? Set.of() : bookmark.values();
        return new RemoteBookmark( serialisedBookmark );
    }
}
