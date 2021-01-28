/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.FlowControl;
import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class PullUpdatesJobTest
{
    private final FlowControl flowControl = mock( FlowControl.class );

    @Test
    void trailingTaskShouldBeIgnoredAfterFailure()
    {
        var batchingTxApplier = new StubBatchingTxApplier();
        batchingTxApplier.startFailing();

        var errors = new MutableInt();
        var batchSize = 1;
        var catchupJob = new PullUpdatesJob( 100, batchSize, new ErrorIncrementingHandler( errors ), batchingTxApplier, NullLog.getInstance(), () -> false );

        var signal = new CompletableFuture<TxStreamFinishedResponse>();
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), batchSize ),
                flowControl );
        assertThat( batchingTxApplier.queuedTxs ).hasSize( 1 );
        assertThat( errors.getValue() ).isEqualTo( 1 );
        assertThat( signal.isCompletedExceptionally() );
    }

    @Test
    void trailingTaskShouldAbortIfCancelSignalIsSent()
    {
        var batchingTxApplier = new StubBatchingTxApplier();
        var shouldAbort = new MutableBoolean( false );

        var errors = new MutableInt();
        long maxBatchSize = 10;
        var catchupJob =
                new PullUpdatesJob( 100, maxBatchSize, new ErrorIncrementingHandler( errors ), batchingTxApplier, NullLog.getInstance(),
                        shouldAbort::booleanValue );

        var signal = new CompletableFuture<TxStreamFinishedResponse>();
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );

        // enable abort signal
        shouldAbort.setTrue();
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );

        assertThat( batchingTxApplier.queuedTxs ).hasSize( 3 );
        assertThat( errors.getValue() ).isEqualTo( 0 );
        assertThat( signal.isCompletedExceptionally() );
        var jobException = assertThrows( ExecutionException.class, signal::get );
        assertThat( jobException.getCause() ).isEqualTo( CancelledPullUpdatesJobException.INSTANCE );
    }

    @Test
    void shouldPullWhenPatchSizeIsReached()
    {
        var batchingTxApplier = new StubBatchingTxApplier();

        var errors = new MutableInt();
        var batchSize = 3;
        var catchupJob = new PullUpdatesJob( 100, batchSize, new ErrorIncrementingHandler( errors ), batchingTxApplier, NullLog.getInstance(), () -> false );

        var signal = new CompletableFuture<TxStreamFinishedResponse>();
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        assertThat( batchingTxApplier.appliedCalls ).isEqualTo( 0 );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 2 ), flowControl );
        assertThat( batchingTxApplier.appliedCalls ).isEqualTo( 1 );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        assertThat( batchingTxApplier.appliedCalls ).isEqualTo( 1 );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        assertThat( batchingTxApplier.appliedCalls ).isEqualTo( 1 );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        assertThat( batchingTxApplier.appliedCalls ).isEqualTo( 2 );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 5 ), flowControl );
        assertThat( batchingTxApplier.appliedCalls ).isEqualTo( 3 );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        assertThat( batchingTxApplier.appliedCalls ).isEqualTo( 3 );
        assertThat( batchingTxApplier.queuedTxs ).hasSize( 7 );
        assertThat( errors.getValue() ).isEqualTo( 0 );
        assertThat( signal.isCompletedExceptionally() );
    }

    @Test
    void shouldStopReadingIfAsyncApplyIsFallingBehind()
    {
        var batchingTxApplier = new StubBatchingTxApplier();
        batchingTxApplier.stopResponding();

        var errors = new MutableInt();
        var queueSize = 4;
        var batchSize = 1; // higher watermark = 4, lower = 2
        var catchupJob =
                new PullUpdatesJob( queueSize, batchSize, new ErrorIncrementingHandler( errors ), batchingTxApplier, NullLog.getInstance(), () -> false );

        var signal = new CompletableFuture<TxStreamFinishedResponse>();
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 2 ), flowControl );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ), flowControl );
        verify( flowControl ).stopReading();
        verify( flowControl, never() ).continueReading();
        batchingTxApplier.startResponding();
        verify( flowControl ).continueReading();
    }

    private static class ErrorIncrementingHandler implements AsyncTaskEventHandler
    {
        private final MutableInt errors;

        ErrorIncrementingHandler( MutableInt errors )
        {
            this.errors = errors;
        }

        @Override
        public void onFailure( Exception e )
        {
            errors.increment();
        }

        @Override
        public void onSuccess()
        {

        }
    }

    private static class StubBatchingTxApplier extends BatchingTxApplier
    {
        LinkedList<CommittedTransactionRepresentation> queuedTxs = new LinkedList<>();
        LinkedList<AsyncTaskEventHandler> waitingToRespond = new LinkedList<>();
        private final Exception onFailure = new RuntimeException();
        private int appliedCalls;
        boolean succeeding = true;
        boolean responding = true;

        StubBatchingTxApplier()
        {
            super( 1, null, new Monitors(), null, null, NullLogProvider.nullLogProvider(), null, null, null );
        }

        @Override
        public synchronized void queue( CommittedTransactionRepresentation tx )
        {
            queuedTxs.add( tx );
        }

        @Override
        public synchronized void applyBatchAsync( AsyncTaskEventHandler asyncTaskEventHandler )
        {
            appliedCalls++;
            if ( !responding )
            {
                waitingToRespond.add( asyncTaskEventHandler );
                return;
            }
            respond( asyncTaskEventHandler );
        }

        private void respond( AsyncTaskEventHandler asyncTaskEventHandler )
        {
            if ( succeeding )
            {
                asyncTaskEventHandler.onSuccess();
            }
            else
            {
                asyncTaskEventHandler.onFailure( onFailure );
            }
        }

        void startFailing()
        {
            succeeding = false;
        }

        void stopResponding()
        {
            responding = false;
        }

        void startResponding()
        {
            responding = false;
            while ( !waitingToRespond.isEmpty() )
            {
                respond( waitingToRespond.poll() );
            }
        }
    }
}
