/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import com.neo4j.fabric.bolt.FabricBookmark;
import com.neo4j.fabric.executor.FabricException;

import java.util.ArrayList;
import java.util.List;

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
public class MixedModeBookmarkManager extends FabricOnlyBookmarkManager
{
    private final LocalGraphTransactionIdTracker localGraphTransactionIdTracker;

    public MixedModeBookmarkManager( LocalGraphTransactionIdTracker localGraphTransactionIdTracker )
    {
        super( localGraphTransactionIdTracker );
        this.localGraphTransactionIdTracker = localGraphTransactionIdTracker;
    }

    @Override
    public void processSubmittedByClient( List<Bookmark> bookmarks )
    {
        var bookmarksByType = sortOutByType( bookmarks );

        processSystemDatabase( bookmarksByType );
        super.processSubmittedByClient( bookmarksByType.fabricBookmarks );
    }

    private BookmarksByType sortOutByType( List<Bookmark> bookmarks )
    {

        long systemDbTxId = -1;
        List<Bookmark> fabricBookmarks = new ArrayList<>( bookmarks.size() );

        for ( var bookmark : bookmarks )
        {
            if ( bookmark instanceof FabricBookmark )
            {
                fabricBookmarks.add( bookmark );
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

    private static class BookmarksByType
    {

        private final long systemDbTxId;
        private final List<Bookmark> fabricBookmarks;

        BookmarksByType( Long systemDbTxId, List<Bookmark> fabricBookmarks )
        {
            this.systemDbTxId = systemDbTxId;
            this.fabricBookmarks = fabricBookmarks;
        }
    }
}
