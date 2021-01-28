/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;

import java.util.concurrent.CompletableFuture;

class SignalEventHandler implements AsyncTaskEventHandler
{
    private final CompletableFuture<TxStreamFinishedResponse> completableFuture;
    private final TxStreamFinishedResponse onSuccess;

    static SignalEventHandler keepRunning( CompletableFuture<TxStreamFinishedResponse> completableFuture )
    {
        return new SignalEventHandler( completableFuture, null );
    }

    static SignalEventHandler completeSignal( CompletableFuture<TxStreamFinishedResponse> completableFuture, TxStreamFinishedResponse response )
    {
        return new SignalEventHandler( completableFuture, response );
    }

    private SignalEventHandler( CompletableFuture<TxStreamFinishedResponse> completableFuture, TxStreamFinishedResponse onSuccess )
    {
        this.completableFuture = completableFuture;
        this.onSuccess = onSuccess;
    }

    @Override
    public void onFailure( Exception e )
    {
        completableFuture.completeExceptionally( e );
    }

    @Override
    public void onSuccess()
    {
        if ( onSuccess != null )
        {
            completableFuture.complete( onSuccess );
        }
    }
}
