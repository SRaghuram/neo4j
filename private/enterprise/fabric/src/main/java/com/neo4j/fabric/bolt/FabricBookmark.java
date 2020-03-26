/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import com.neo4j.fabric.driver.RemoteBookmark;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.neo4j.values.storable.Values.utf8Value;

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
    public NamedDatabaseId databaseId()
    {
        return getNamedDatabaseId();
    }

    @Override
    public void attachTo( BoltResponseHandler state )
    {
        state.onMetadata( BOOKMARK_KEY, utf8Value( serialize() ) );
    }

    public List<GraphState> getGraphStates()
    {
        return graphStates;
    }

    @Override
    public Bookmark toBookmark( BiFunction<Long,NamedDatabaseId, Bookmark> defaultBookmarkFormat )
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
        private final List<RemoteBookmark> bookmarks;

        public GraphState( long remoteGraphId, List<RemoteBookmark> bookmarks )
        {
            this.remoteGraphId = remoteGraphId;
            this.bookmarks = bookmarks;
        }

        public long getRemoteGraphId()
        {
            return remoteGraphId;
        }

        public List<RemoteBookmark> getBookmarks()
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
                    .map( GraphState::serialize0 )
                    .collect( Collectors.joining( ",", remoteGraphId + ":", "" ) );
        }

        private static String serialize0( RemoteBookmark remoteBookmark )
        {
            return remoteBookmark.getSerialisedState().stream()
                    .map( bookmarkPart -> Base64.getEncoder().encodeToString( bookmarkPart.getBytes( StandardCharsets.UTF_8 ) ) )
                    .collect( Collectors.joining( "|" ) );
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
