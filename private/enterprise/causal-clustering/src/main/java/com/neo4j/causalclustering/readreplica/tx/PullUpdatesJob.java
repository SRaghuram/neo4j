/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.IncomingResponseValve;
import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.readreplica.tx.SignalEventHandler.completeSignal;
import static com.neo4j.causalclustering.readreplica.tx.SignalEventHandler.keepRunning;
import static java.lang.StrictMath.max;

public class PullUpdatesJob extends CatchupResponseAdaptor<TxStreamFinishedResponse>
{
    private final long upperWatermark;
    private final long lowerWatermark;
    private final long maxBatchSize;
    private final AsyncTaskEventHandler applyFailureHandler;
    private final BatchingTxApplier batchingTxApplier;
    private final Log log;
    private final Aborter cancelSignal;
    private final TrackingFailureHandler trackingFailureHandler = new TrackingFailureHandler();
    private IncomingResponseValveController incomingResponseValveController;
    private int currentBatchSize;

    public PullUpdatesJob( long maxQueueSize, long maxBatchSize, AsyncTaskEventHandler applyFailureHandler, BatchingTxApplier batchingTxApplier, Log log,
            Aborter cancelSignal )
    {
        this.upperWatermark = max( 1, maxQueueSize / maxBatchSize );
        this.lowerWatermark = upperWatermark / 2;
        this.maxBatchSize = maxBatchSize;
        this.applyFailureHandler = applyFailureHandler;
        this.batchingTxApplier = batchingTxApplier;
        this.log = log;
        this.cancelSignal = cancelSignal;
    }

    @Override
    public void onTxPullResponse( CompletableFuture<TxStreamFinishedResponse> signal, ReceivedTxPullResponse response,
            IncomingResponseValve incomingResponseValve )
    {
        if ( incomingResponseValveController == null )
        {
            incomingResponseValveController = new IncomingResponseValveController( upperWatermark, lowerWatermark, incomingResponseValve );
        }
        batchingTxApplier.queue( response.tx() );
        currentBatchSize += response.txSize();

        if ( cancelSignal.shouldAbort() )
        {
            signal.completeExceptionally( CancelledPullUpdatesJobException.INSTANCE );
        }
        else if ( currentBatchSize >= maxBatchSize )
        {
            incomingResponseValveController.scheduledJob();
            batchingTxApplier.applyBatchAsync( ongoingJobEventHandler( signal ) );
            currentBatchSize = 0;
        }
    }

    @Override
    public void onTxStreamFinishedResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxStreamFinishedResponse response )
    {
        batchingTxApplier.applyBatchAsync( completeJobEventHandler( signal, response ) );
    }

    @Override
    public void onCatchupErrorResponse( CompletableFuture<TxStreamFinishedResponse> signal, CatchupErrorResponse catchupErrorResponse )
    {
        signal.complete( new TxStreamFinishedResponse( catchupErrorResponse.status(), -1 ) );
        log.warn( catchupErrorResponse.message() );
    }

    private AsyncTaskEventHandler completeJobEventHandler( CompletableFuture<TxStreamFinishedResponse> signal, TxStreamFinishedResponse response )
    {
        return new CompositeAsyncEventHandler( List.of( applyFailureHandler, trackingFailureHandler, completeSignal( signal, response ) ) );
    }

    private AsyncTaskEventHandler ongoingJobEventHandler( CompletableFuture<TxStreamFinishedResponse> signal )
    {
        return new CompositeAsyncEventHandler(
                List.of( incomingResponseValveController, applyFailureHandler, trackingFailureHandler, keepRunning( signal ) ) );
    }
}
