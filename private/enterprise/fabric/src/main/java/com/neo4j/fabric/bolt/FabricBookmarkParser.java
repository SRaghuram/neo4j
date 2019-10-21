/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import java.util.List;

import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.runtime.Bookmark;

public class FabricBookmarkParser implements CustomBookmarkFormatParser
{
    @Override
    public boolean isCustomBookmark( String string )
    {
        return false;
    }

    @Override
    public List<Bookmark> parse( List<String> customBookmarks, long systemDbTxId )
    {
        return null;
    }
}
