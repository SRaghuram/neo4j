/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import com.neo4j.fabric.bolt.FabricBookmark;
import com.neo4j.fabric.driver.RemoteBookmark;
import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.executor.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * This transaction bookmark manger is used when fabric bookmarks are used only for "Fabric" database.
 * In other words, non-fabric databases are not using fabric bookmarks.
 * <p>
 * The most specific thing about this transaction bookmark manger is that it handles mixture of fabric and non-fabric bookmarks
 * as it must handle System database bookmarks, which are not fabric ones.
 */
public class MixedModeBookmarkManager implements TransactionBookmarkManager
{
    private final LocalGraphTransactionIdTracker localGraphTransactionIdTracker;

    private final Map<UUID, GraphBookmarkData> remoteGraphBookmarkData = new ConcurrentHashMap<>();

    public MixedModeBookmarkManager( LocalGraphTransactionIdTracker localGraphTransactionIdTracker )
    {
        this.localGraphTransactionIdTracker = localGraphTransactionIdTracker;
    }

    @Override
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

    @Override
    public List<RemoteBookmark> getBookmarksForRemote( Location.Remote location )
    {
        var bookmarkData = remoteGraphBookmarkData.get( location.getUuid() );

        if ( bookmarkData != null )
        {
            return bookmarkData.bookmarksSubmittedByClient;
        }

        return List.of();
    }

    @Override
    public void remoteTransactionCommitted( Location.Remote location, RemoteBookmark bookmark )
    {
        var bookmarkData = remoteGraphBookmarkData.computeIfAbsent( location.getUuid(), g -> new GraphBookmarkData() );

        // there must be only one transaction per location,
        // so there is something fishy when more than bookmark is registered from the same location
        if ( bookmarkData.bookmarkReceivedFromGraph != null )
        {
            throw new IllegalStateException( "More that one bookmark received from a remote location " + location );
        }

        bookmarkData.bookmarkReceivedFromGraph = bookmark;
    }

    @Override
    public void awaitUpToDate( Location.Local location )
    {
        // no-op as fabric database does not support writes
    }

    @Override
    public void localTransactionCommitted( Location.Local local )
    {
        // no-op as fabric database does not support writes
    }

    public FabricBookmark constructFinalBookmark()
    {
        var remoteStates = remoteGraphBookmarkData.entrySet().stream().map( entry -> {

            var bookmarkData = entry.getValue();
            List<RemoteBookmark> graphBookmarks;
            if ( bookmarkData.bookmarkReceivedFromGraph != null )
            {
                graphBookmarks = List.of( bookmarkData.bookmarkReceivedFromGraph );
            }
            else
            {
                graphBookmarks = bookmarkData.bookmarksSubmittedByClient;
            }

            return new FabricBookmark.ExternalGraphState( entry.getKey(), graphBookmarks );
        } ).collect( Collectors.toList() );

        return new FabricBookmark( List.of(), remoteStates );
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
                if ( !bookmark.databaseId().equals( NAMED_SYSTEM_DATABASE_ID ) )
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
            localGraphTransactionIdTracker.awaitSystemGraphUpToDate( bookmarksByType.systemDbTxId );
        }
    }

    private void processFabricBookmarks( BookmarksByType bookmarksByType )
    {
        for ( var bookmark : bookmarksByType.fabricBookmarks )
        {
            bookmark.getExternalGraphStates().forEach( graphState ->
            {
                var bookmarkData = remoteGraphBookmarkData.computeIfAbsent( graphState.getGraphUuid(), g -> new GraphBookmarkData() );
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
        private final List<RemoteBookmark> bookmarksSubmittedByClient = new ArrayList<>();
        private RemoteBookmark bookmarkReceivedFromGraph;
    }
}
