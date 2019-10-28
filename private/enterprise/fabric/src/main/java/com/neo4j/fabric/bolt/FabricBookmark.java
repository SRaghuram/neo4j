/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.values.storable.Values.stringValue;

public class FabricBookmark extends BookmarkMetadata implements Bookmark
{
    public static final String PREFIX = "FB:";

    private final List<GraphState> graphStates;

    public FabricBookmark( List<GraphState> graphStates )
    {
        super( -1, null );

        this.graphStates = graphStates;
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
        state.onMetadata( BOOKMARK_KEY, stringValue( serialize() ) );
    }

    public List<GraphState> getGraphStates()
    {
        return graphStates;
    }

    public Bookmark toBookmark( BiFunction<Long, DatabaseId, Bookmark> defaultBookmarkFormat )
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "FabricBookmark{" + "graphStates=" + graphStates + '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        FabricBookmark that = (FabricBookmark) o;
        return graphStates.equals( that.graphStates );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( graphStates );
    }

    public String serialize()
    {
        return graphStates.stream().map( GraphState::serialize ).collect( Collectors.joining( "-", PREFIX, "" ) );
    }

    public static class GraphState
    {
        private final long remoteGraphId;
        private final List<String> bookmarks;

        public GraphState( long remoteGraphId, List<String> bookmarks )
        {
            this.remoteGraphId = remoteGraphId;
            this.bookmarks = bookmarks;
        }

        public long getRemoteGraphId()
        {
            return remoteGraphId;
        }

        public List<String> getBookmarks()
        {
            return bookmarks;
        }

        @Override
        public String toString()
        {
            return "GraphState{" + "remoteGraphId=" + remoteGraphId + ", bookmarks=" + bookmarks + '}';
        }

        private String serialize()
        {
            return bookmarks.stream()
                    .map( bookmark -> Base64.getEncoder().encodeToString( bookmark.getBytes( StandardCharsets.UTF_8 ) ) )
                    .collect( Collectors.joining( ",", remoteGraphId + ":", "" ) );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            GraphState that = (GraphState) o;
            return remoteGraphId == that.remoteGraphId && bookmarks.equals( that.bookmarks );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( remoteGraphId, bookmarks );
        }
    }
}
