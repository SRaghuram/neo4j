/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.readreplica.BatchingTxApplier;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PullUpdatesJobTest
{
    private final LogProvider logProvider = NullLogProvider.nullLogProvider();

    @Test
    void trailingTaskShouldBeIgnoredAfterFailure() throws Exception
    {
        var jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        var asyncTxApplier = new AsyncTxApplier( jobScheduler, logProvider );
        asyncTxApplier.start();
        var batchingTxApplier = mock( BatchingTxApplier.class );
        doThrow( RuntimeException.class ).when( batchingTxApplier ).queue( any(), anyInt() );

        MutableInt errors = new MutableInt();
        var catchupJob = new PullUpdatesJob( e -> errors.increment(), batchingTxApplier, asyncTxApplier, NullLog.getInstance(), () -> false );

        var signal = new CompletableFuture<TxStreamFinishedResponse>();
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        asyncTxApplier.stop();

        verify( batchingTxApplier, times( 1 ) ).queue( any(), anyInt() );
        assertThat( errors.getValue() ).isEqualTo( 1 );
        assertThat( signal.isCompletedExceptionally() );

        jobScheduler.shutdown();
    }

    @Test
    void trailingTaskShouldAbortIfCancelSignalIsSent() throws Exception
    {
        var jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        var asyncTxApplier = new AsyncTxApplier( jobScheduler, logProvider );
        asyncTxApplier.start();
        var batchingTxApplier = mock( BatchingTxApplier.class );
        MutableBoolean shouldAbort = new MutableBoolean( false );

        MutableInt errors = new MutableInt();
        var catchupJob = new PullUpdatesJob( e -> errors.increment(), batchingTxApplier, asyncTxApplier, NullLog.getInstance(), shouldAbort::booleanValue );

        var signal = new CompletableFuture<TxStreamFinishedResponse>();
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        // wait for async jobs to be handled
        var latch = new CountDownLatch( 1 );
        asyncTxApplier.add( latch::countDown );
        latch.await();

        // enable abort signal
        shouldAbort.setTrue();
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        asyncTxApplier.stop();

        verify( batchingTxApplier, times( 3 ) ).queue( any(), anyInt() );
        assertThat( errors.getValue() ).isEqualTo( 0 );
        assertThat( signal.isCompletedExceptionally() );
        var jobException = assertThrows( ExecutionException.class, signal::get );
        assertThat( jobException.getCause() ).isEqualTo( CancelledPullUpdatesJobException.INSTANCE );

        jobScheduler.shutdown();
    }

    @Test
    void queuedTasksShouldBeCompletedOnCancel() throws Exception
    {
        var jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        var asyncTxApplier = new AsyncTxApplier( jobScheduler, logProvider );
        asyncTxApplier.start();
        var batchingTxApplier = mock( BatchingTxApplier.class );
        MutableBoolean shouldAbort = new MutableBoolean( false );

        MutableInt errors = new MutableInt();
        var catchupJob = new PullUpdatesJob( e -> errors.increment(), batchingTxApplier, asyncTxApplier, NullLog.getInstance(), shouldAbort::booleanValue );

        var startLatch = new BinaryLatch();
        asyncTxApplier.add( startLatch::await );

        var signal = new CompletableFuture<TxStreamFinishedResponse>();
        // asyncTxApplier has not started so jobs will be queued but not executed
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );

        // enable abort signal
        shouldAbort.setTrue();
        // the cancel signal will be detected when handling this tx. It will be handled as it has already been serialised, and signal should be completed.
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );
        catchupJob.onTxPullResponse( signal, new ReceivedTxPullResponse( StoreId.UNKNOWN, mock( CommittedTransactionRepresentation.class ), 1 ) );

        // allow jobs to start executing
        startLatch.release();
        asyncTxApplier.stop();

        // note that the transaction received after the cancelled is applied. This is because it has already been recieved when cancel event is detected as
        // we don't react immediately to cancel events.
        verify( batchingTxApplier, times( 3 ) ).queue( any(), anyInt() );
        assertThat( errors.getValue() ).isEqualTo( 0 );
        assertThat( signal.isDone() );

        jobScheduler.shutdown();
    }
}
