/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import java.util.List;
import java.util.function.BiFunction;

import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.values.storable.Values.stringValue;

public class FabricBookmark extends BookmarkMetadata implements Bookmark
{
    private static String BOOKMARK_KEY = "bookmark";

    private final List<RemoteState> remoteStates;

    public FabricBookmark( DatabaseId databaseId, List<RemoteState> remoteStates )
    {
        super( -1, databaseId );

        this.remoteStates = remoteStates;
    }

    @Override
    public long txId()
    {
        return getTxId();
    }

    @Override
    public DatabaseId databaseId()
    {
        return getDatabaseId();
    }

    @Override
    public void attachTo( BoltResponseHandler state )
    {
        state.onMetadata( BOOKMARK_KEY, stringValue( "f:" ) );
    }

    public List<RemoteState> getRemoteStates()
    {
        return remoteStates;
    }

    public Bookmark toBookmark( BiFunction<Long, DatabaseId, Bookmark> defaultBookmarkFormat )
    {
        return this;
    }

    public class RemoteState
    {
        private final int remoteGraphId;
        private final String bookmark;

        public RemoteState( int remoteGraphId, String bookmark )
        {
            this.remoteGraphId = remoteGraphId;
            this.bookmark = bookmark;
        }

        public int getRemoteGraphId()
        {
            return remoteGraphId;
        }

        public String getBookmark()
        {
            return bookmark;
        }
    }
}
