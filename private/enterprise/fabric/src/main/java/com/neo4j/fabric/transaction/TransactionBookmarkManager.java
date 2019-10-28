/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

import com.neo4j.fabric.bolt.FabricBookmark;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

public class TransactionBookmarkManager
{
    private final FabricConfig fabricConfig;
    private final TransactionIdTracker transactionIdTracker;
    private final Duration bookmarkTimeout;

    private final Map<FabricConfig.Graph, GraphBookmarkData> graphBookmarkData = new ConcurrentHashMap<>();

    public TransactionBookmarkManager( FabricConfig fabricConfig, TransactionIdTracker transactionIdTracker, Duration bookmarkTimeout )
    {
        this.fabricConfig = fabricConfig;
        this.transactionIdTracker = transactionIdTracker;
        this.bookmarkTimeout = bookmarkTimeout;
    }

    public void processSubmittedByClient( List<Bookmark> bookmarks )
    {
        if ( bookmarks.isEmpty() )
        {
            return;
        }

        var bookmarksByType = sortOutByType( bookmarks );
        processSystemDatabase( bookmarksByType );
        processFabricBookmarks( bookmarksByType );
    }

    public List<String> getBookmarksForGraph( FabricConfig.Graph graph )
    {
        var bookmarkData = graphBookmarkData.get( graph );

        if ( bookmarkData != null )
        {
            return bookmarkData.bookmarksSubmittedByClient;
        }

        return List.of();
    }

    public void recordBookmarkReceivedFromGraph( FabricConfig.Graph graph, String bookmark )
    {
        var bookmarkData = graphBookmarkData.computeIfAbsent( graph, g -> new GraphBookmarkData() );
        bookmarkData.bookmarkReceivedFromGraph = bookmark;
    }

    public FabricBookmark constructFinalBookmark()
    {
        var remoteStates = graphBookmarkData.entrySet().stream().map( entry -> {

            var bookmarkData = entry.getValue();
            List<String> graphBookmarks;
            if ( bookmarkData.bookmarkReceivedFromGraph != null )
            {
                graphBookmarks = List.of( bookmarkData.bookmarkReceivedFromGraph );
            }
            else
            {
                graphBookmarks = bookmarkData.bookmarksSubmittedByClient;
            }

            return new FabricBookmark.GraphState( entry.getKey().getId(), graphBookmarks );
        } ).collect( Collectors.toList() );

        return new FabricBookmark( remoteStates );
    }

    private BookmarksByType sortOutByType( List<Bookmark> bookmarks )
    {

        long systemDbTxId = -1;
        List<FabricBookmark> fabricBookmarks = new ArrayList<>( bookmarks.size() );

        for ( var bookmark : bookmarks )
        {
            if ( bookmark instanceof FabricBookmark )
            {
                fabricBookmarks.add( (FabricBookmark) bookmark );
            }
            else
            {
                if ( !bookmark.databaseId().equals( SYSTEM_DATABASE_ID ) )
                {
                    throw new FabricException( Status.Transaction.InvalidBookmarkMixture, "Bookmark for unexpected database encountered: " + bookmark );
                }

                systemDbTxId = Math.max( systemDbTxId, bookmark.txId() );
            }
        }

        return new BookmarksByType( systemDbTxId, fabricBookmarks );
    }

    private void processSystemDatabase( BookmarksByType bookmarksByType )
    {
        if ( bookmarksByType.systemDbTxId != -1 )
        {
            transactionIdTracker.awaitUpToDate( SYSTEM_DATABASE_ID, bookmarksByType.systemDbTxId, bookmarkTimeout );
        }
    }

    private void processFabricBookmarks( BookmarksByType bookmarksByType )
    {
        Map<Long, FabricConfig.Graph> graphsById = fabricConfig.getDatabase()
                .getGraphs()
                .stream()
                .collect( Collectors.toMap(FabricConfig.Graph::getId, Function.identity()) );

        for ( var bookmark : bookmarksByType.fabricBookmarks )
        {
            bookmark.getGraphStates().forEach( graphState ->
            {
                var graph = graphsById.get( graphState.getRemoteGraphId() );

                if ( graph == null )
                {
                    throw new FabricException( Status.Transaction.InvalidBookmark,
                            "Bookmark with non-existent remote graph ID database encountered: " + bookmark );
                }

                var bookmarkData = graphBookmarkData.computeIfAbsent( graph, g -> new GraphBookmarkData() );
                bookmarkData.bookmarksSubmittedByClient.addAll( graphState.getBookmarks() );
            } );
        }
    }

    private static class BookmarksByType
    {

        private final long systemDbTxId;
        private final List<FabricBookmark> fabricBookmarks;

        BookmarksByType( Long systemDbTxId, List<FabricBookmark> fabricBookmarks )
        {
            this.systemDbTxId = systemDbTxId;
            this.fabricBookmarks = fabricBookmarks;
        }
    }

    private static class GraphBookmarkData
    {
        private final List<String> bookmarksSubmittedByClient = new ArrayList<>();
        private String bookmarkReceivedFromGraph;
    }
}
