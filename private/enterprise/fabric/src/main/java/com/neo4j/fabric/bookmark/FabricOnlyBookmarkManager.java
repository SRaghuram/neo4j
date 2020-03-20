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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.api.exceptions.Status;

public class FabricOnlyBookmarkManager implements TransactionBookmarkManager
{
    private final FabricBookmarkParser fabricBookmarkParser = new FabricBookmarkParser();
    private final LocalGraphTransactionIdTracker transactionIdTracker;

    private final Map<UUID, List<RemoteBookmark>> bookmarksFromExternalGraphs = new ConcurrentHashMap<>();
    private final Map<UUID,Long> txIdsFromInternalGraphs = new ConcurrentHashMap<>();

    private volatile Map<UUID,Long> requestedInternalGraphTxIds;
    private volatile Map<UUID, List<RemoteBookmark>> requestedExternalGraphStates;
    private volatile RemoteBookmark interClusterBookmark;

    public FabricOnlyBookmarkManager( LocalGraphTransactionIdTracker transactionIdTracker )
    {
        this.transactionIdTracker = transactionIdTracker;
    }

    @Override
    public void processSubmittedByClient( List<Bookmark> bookmarks )
    {
        var fabricBookmarks = convert( bookmarks );
        this.requestedInternalGraphTxIds = getInternalGraphTxIds( fabricBookmarks );
        this.requestedExternalGraphStates = getExternalGraphStates( fabricBookmarks );
        // regardless of what we do, System graph must be always up to date
        awaitSystemGraphUpTpDate();
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

    private Map<UUID,Long> getInternalGraphTxIds( List<FabricBookmark> fabricBookmarks )
    {
        Map<UUID,Long> internalGraphTxIds = new HashMap<>();

        fabricBookmarks.stream()
                .flatMap( fabricBookmark -> fabricBookmark.getInternalGraphStates().stream() )
                .forEach( internalGraphState -> internalGraphTxIds.merge( internalGraphState.getGraphUuid(),
                        internalGraphState.getTransactionId(),
                        Math::max )
                );

        return internalGraphTxIds;
    }

    private Map<UUID,List<RemoteBookmark>> getExternalGraphStates( List<FabricBookmark> fabricBookmarks )
    {
        Map<UUID,List<RemoteBookmark>> externalGraphStates = new HashMap<>();
        fabricBookmarks.stream()
                .flatMap( fabricBookmark -> fabricBookmark.getExternalGraphStates().stream() )
                .forEach( externalGraphState -> externalGraphStates.computeIfAbsent( externalGraphState.getGraphUuid(), key -> new ArrayList<>() )
                        .addAll( externalGraphState.getBookmarks() )
                );
        return externalGraphStates;
    }

    private void awaitSystemGraphUpTpDate()
    {
        transactionIdTracker.awaitSystemGraphUpToDate( requestedInternalGraphTxIds );
    }

    @Override
    public List<RemoteBookmark> getBookmarksForRemote( Location.Remote location )
    {
        if ( location instanceof Location.Remote.External )
        {
            return requestedExternalGraphStates.getOrDefault( location.getUuid(), List.of() );
        }

        if ( interClusterBookmark == null )
        {
            constructInterClusterBookmark();
        }

        return List.of( interClusterBookmark );
    }

    private void constructInterClusterBookmark()
    {
        // The inter-cluster remote needs the same bookmark data that was submitted to the this DBMS,
        // so whatever was received in 'processSubmittedByClient' can be simply passed on.
        // However, since the raw bookmark data has already been processed and only the useful stuff kept,
        // it is better to create a bookmark from that
        var internalGraphStates = requestedInternalGraphTxIds.entrySet().stream()
                .map( entry -> new FabricBookmark.InternalGraphState( entry.getKey(), entry.getValue() ) )
                .collect( Collectors.toList());
        var externalGraphStates = requestedExternalGraphStates.entrySet().stream()
                .map( entry -> new FabricBookmark.ExternalGraphState( entry.getKey(), entry.getValue() ) )
                .collect( Collectors.toList());

        var fabricBookmark = new FabricBookmark( internalGraphStates, externalGraphStates );
        interClusterBookmark = new RemoteBookmark( fabricBookmark.serialize() );
    }

    @Override
    public void remoteTransactionCommitted( Location.Remote location, RemoteBookmark bookmark )
    {
        if ( bookmark == null )
        {
            return;
        }

        if ( location instanceof Location.Remote.External )
        {
            bookmarksFromExternalGraphs.computeIfAbsent( location.getUuid(), key -> new ArrayList<>() ).add( bookmark );
        }
        else
        {
            FabricBookmark fabricBookmark = fabricBookmarkParser.parse( bookmark.getSerialisedState() );
            fabricBookmark.getExternalGraphStates().forEach( externalGraphState ->
                    bookmarksFromExternalGraphs.computeIfAbsent( externalGraphState.getGraphUuid(), key -> new ArrayList<>() )
                    .addAll( externalGraphState.getBookmarks() ) );

            fabricBookmark.getInternalGraphStates()
                    .forEach( internalGraphState ->
                            txIdsFromInternalGraphs.merge( internalGraphState.getGraphUuid(), internalGraphState.getTransactionId(), Math::max ) );
        }
    }

    @Override
    public void awaitUpToDate( Location.Local location )
    {
        Long transactionId = requestedInternalGraphTxIds.get( location.getUuid() );
        if ( transactionId != null )
        {
            transactionIdTracker.awaitGraphUpToDate( location, transactionId);
        }
    }

    @Override
    public void localTransactionCommitted( Location.Local location )
    {
        long transactionId = transactionIdTracker.getTransactionId( location );
        txIdsFromInternalGraphs.put( location.getUuid(), transactionId );
    }

    @Override
    public FabricBookmark constructFinalBookmark()
    {
        var internalGraphStates = createInternalGraphStates();
        var externalGraphStates = createExternalGraphStates();
        return new FabricBookmark( internalGraphStates, externalGraphStates );
    }

    private List<FabricBookmark.InternalGraphState> createInternalGraphStates()
    {
        Map<UUID,Long> internalGraphTxIds = new HashMap<>( txIdsFromInternalGraphs );
        requestedInternalGraphTxIds.forEach( internalGraphTxIds::putIfAbsent );

        return internalGraphTxIds.entrySet().stream()
                .map( entry -> new FabricBookmark.InternalGraphState( entry.getKey(), entry.getValue() ) )
                .collect( Collectors.toList());
    }

    private List<FabricBookmark.ExternalGraphState> createExternalGraphStates()
    {
        Map<UUID,List<RemoteBookmark>> externalGraphTxIds = new HashMap<>( bookmarksFromExternalGraphs );
        requestedExternalGraphStates.forEach( externalGraphTxIds::putIfAbsent );

        return externalGraphTxIds.entrySet().stream()
                .map( entry -> new  FabricBookmark.ExternalGraphState( entry.getKey(), entry.getValue() ) )
                .collect( Collectors.toList());
    }
}
