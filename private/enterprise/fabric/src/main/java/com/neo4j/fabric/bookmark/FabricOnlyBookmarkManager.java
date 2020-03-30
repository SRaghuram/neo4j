/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import com.neo4j.fabric.bolt.FabricBookmark;
import com.neo4j.fabric.bolt.FabricBookmarkParser;
import com.neo4j.fabric.driver.RemoteBookmark;
import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.executor.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.api.exceptions.Status;

public class FabricOnlyBookmarkManager implements TransactionBookmarkManager
{
    private final FabricBookmarkParser fabricBookmarkParser = new FabricBookmarkParser();
    private final LocalGraphTransactionIdTracker transactionIdTracker;

    // must be taken when updating the final bookmark
    private final Object finalBookmarkLock = new Object();

    private volatile FabricBookmark submittedBookmark;
    private volatile FabricBookmark finalBookmark;

    public FabricOnlyBookmarkManager( LocalGraphTransactionIdTracker transactionIdTracker )
    {
        this.transactionIdTracker = transactionIdTracker;
    }

    @Override
    public void processSubmittedByClient( List<Bookmark> bookmarks )
    {
        var fabricBookmarks = convert( bookmarks );
        this.submittedBookmark = FabricBookmark.merge( fabricBookmarks );
        this.finalBookmark = new FabricBookmark(
                new ArrayList<>( submittedBookmark.getInternalGraphStates() ),
                new ArrayList<>( submittedBookmark.getExternalGraphStates() )
        );
        // regardless of what we do, System graph must be always up to date
        awaitSystemGraphUpToDate();
    }

    private List<FabricBookmark> convert( List<Bookmark> bookmarks )
    {
        return bookmarks.stream().map( bookmark ->
        {
            if ( !(bookmark instanceof FabricBookmark) )
            {
                throw new FabricException( Status.Transaction.InvalidBookmarkMixture, "Bookmark of unexpected type encountered: " + bookmark );
            }

            return (FabricBookmark) bookmark;
        } ).collect( Collectors.toList() );
    }

    private void awaitSystemGraphUpToDate()
    {
        var graphUuid2TxIdMapping = submittedBookmark.getInternalGraphStates().stream()
                .collect( Collectors.toMap( FabricBookmark.InternalGraphState::getGraphUuid, FabricBookmark.InternalGraphState::getTransactionId) );
        transactionIdTracker.awaitSystemGraphUpToDate( graphUuid2TxIdMapping );
    }

    @Override
    public List<RemoteBookmark> getBookmarksForRemote( Location.Remote location )
    {
        if ( location instanceof Location.Remote.External )
        {
            return submittedBookmark.getExternalGraphStates().stream()
                    .filter( egs -> egs.getGraphUuid().equals( location.getUuid() ) )
                    .map( FabricBookmark.ExternalGraphState::getBookmarks )
                    .findAny()
                    .orElse( List.of() );
        }

        // The inter-cluster remote needs the same bookmark data that was submitted to the this DBMS
        return List.of( new RemoteBookmark( submittedBookmark.serialize() ) );
    }

    @Override
    public void remoteTransactionCommitted( Location.Remote location, RemoteBookmark bookmark )
    {
        if ( bookmark == null )
        {
            return;
        }

        synchronized ( finalBookmarkLock )
        {
            if ( location instanceof Location.Remote.External )
            {
                var externalGraphState = new FabricBookmark.ExternalGraphState( location.getUuid(), List.of( bookmark ) );
                var bookmarkUpdate = new FabricBookmark( List.of(), List.of( externalGraphState ) );
                finalBookmark = FabricBookmark.merge( List.of( finalBookmark, bookmarkUpdate ) );
            }
            else
            {
                var fabricBookmark = fabricBookmarkParser.parse( bookmark.getSerialisedState() );
                finalBookmark = FabricBookmark.merge( List.of( finalBookmark, fabricBookmark ) );
            }
        }
    }

    @Override
    public void awaitUpToDate( Location.Local location )
    {
        submittedBookmark.getInternalGraphStates().stream()
                .filter( egs -> egs.getGraphUuid().equals( location.getUuid() ) )
                .map( FabricBookmark.InternalGraphState::getTransactionId )
                .findAny()
                .ifPresent( transactionId -> transactionIdTracker.awaitGraphUpToDate( location, transactionId ) );
    }

    @Override
    public void localTransactionCommitted( Location.Local location )
    {
        synchronized ( finalBookmarkLock )
        {
            long transactionId = transactionIdTracker.getTransactionId( location );
            var internalGraphState = new FabricBookmark.InternalGraphState( location.getUuid(), transactionId );
            var bookmarkUpdate = new FabricBookmark( List.of( internalGraphState ), List.of() );
            finalBookmark = FabricBookmark.merge( List.of( finalBookmark, bookmarkUpdate ) );
        }
    }

    @Override
    public FabricBookmark constructFinalBookmark()
    {
        return finalBookmark;
    }
}
