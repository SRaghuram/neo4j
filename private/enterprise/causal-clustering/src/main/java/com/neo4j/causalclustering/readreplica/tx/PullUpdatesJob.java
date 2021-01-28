/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.readreplica.BatchingTxApplier;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import org.neo4j.logging.Log;

public class PullUpdatesJob extends CatchupResponseAdaptor<TxStreamFinishedResponse>
{
    private final FailureEventHandler applyFailureHandler;
    private final BatchingTxApplier batchingTxApplier;
    private final AsyncTxApplier asyncTxApplier;
    private final Log log;
    private final Aborter cancelSignal;
    private final TrackingFailureHandler trackingFailureHandler = new TrackingFailureHandler();
    private boolean hasCancelled;

    public PullUpdatesJob( FailureEventHandler applyFailureHandler, BatchingTxApplier batchingTxApplier, AsyncTxApplier asyncTxApplier, Log log,
            Aborter cancelSignal )
    {
        this.applyFailureHandler = applyFailureHandler;
        this.batchingTxApplier = batchingTxApplier;
        this.asyncTxApplier = asyncTxApplier;
        this.log = log;
        this.cancelSignal = cancelSignal;
    }

    @Override
    public void onTxPullResponse( CompletableFuture<TxStreamFinishedResponse> signal, ReceivedTxPullResponse response )
    {
        if ( hasCancelled )
        {
            return;
        }

        queueJob( () ->
        {
            batchingTxApplier.queue( response.tx(), response.txSize() );
            return null;
        }, signal );

        if ( cancelSignal.shouldAbort() )
        {
            queueJob( () ->
            {
                // signal is completed exceptionally so that it gets disposed,
                // but only after applier is completed so that the catchup process blocks until completion
                signal.completeExceptionally( CancelledPullUpdatesJobException.INSTANCE );
                return null;
            }, signal );
            hasCancelled = true;
        }
    }

    @Override
    public void onTxStreamFinishedResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxStreamFinishedResponse response )
    {
        queueJob( () ->
        {
            batchingTxApplier.applyBatch();
            signal.complete( response );
            return null;
        }, signal );
    }

    @Override
    public void onCatchupErrorResponse( CompletableFuture<TxStreamFinishedResponse> signal, CatchupErrorResponse catchupErrorResponse )
    {
        signal.complete( new TxStreamFinishedResponse( catchupErrorResponse.status(), -1 ) );
        log.warn( catchupErrorResponse.message() );
    }

    void queueJob( Callable<Void> task, CompletableFuture<TxStreamFinishedResponse> signal )
    {
        asyncTxApplier.add( new AsyncTask( task, trackingFailureHandler, failureEvents( signal ) ) );
    }

    private FailureEventHandler failureEvents( CompletableFuture<TxStreamFinishedResponse> signal )
    {
        return new CompositeFailureEventHandler( List.of( applyFailureHandler, trackingFailureHandler, new CloseSignalFailureHandler( signal ) ) );
    }
}
