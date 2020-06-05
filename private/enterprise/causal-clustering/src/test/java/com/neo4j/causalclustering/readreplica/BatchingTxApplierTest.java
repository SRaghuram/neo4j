/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

class BatchingTxApplierTest
{
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );
    private final TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
    private final CommandIndexTracker commandIndexTracker = new CommandIndexTracker();

    private final long startTxId = 31L;
    private final int maxBatchSize = 16;

    private final ReplicatedDatabaseEventDispatch databaseEventDispatch = mock( ReplicatedDatabaseEventDispatch.class );
    private final TrackingPageCacheTracer pageCacheTracer = new TrackingPageCacheTracer();
    private final BatchingTxApplier txApplier = new BatchingTxApplier( maxBatchSize, () -> idStore, () -> commitProcess,
            new Monitors(), EmptyVersionContextSupplier.EMPTY, commandIndexTracker,
            nullLogProvider(), databaseEventDispatch, pageCacheTracer );

    @BeforeEach
    void before() throws TransactionFailureException
    {
        when( commitProcess.commit( any(), any(), any() ) ).thenAnswer( invocation ->
        {
            TransactionToApply tx = invocation.getArgument( 0 );
            while ( tx != null )
            {
                tx.close();
                tx = tx.next();
            }
            return -1L;
        } );
        when( idStore.getLastCommittedTransactionId() ).thenReturn( startTxId );
        when( idStore.getLastClosedTransactionId() ).thenReturn( startTxId );
        txApplier.start();
    }

    @AfterEach
    void after()
    {
        txApplier.stop();
    }

    @Test
    void shouldHaveCorrectDefaults()
    {
        assertEquals( startTxId, txApplier.lastQueuedTxId() );
    }

    @Test
    void shouldApplyBatchAndDispatchEvents() throws Exception
    {
        // given
        txApplier.queue( createTxWithId( startTxId + 1 ) );
        txApplier.queue( createTxWithId( startTxId + 2 ) );
        txApplier.queue( createTxWithId( startTxId + 3 ) );

        // when
        txApplier.applyBatch();

        // then
        assertEquals( startTxId + 3, txApplier.lastQueuedTxId() );
        assertTransactionsCommitted( startTxId + 1, 3 );
    }

    @Test
    void shouldIgnoreOutOfOrderTransactions() throws Exception
    {
        // given
        txApplier.queue( createTxWithId( startTxId + 4 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 1 ) );
        txApplier.queue( createTxWithId( startTxId + 3 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 2 ) );
        txApplier.queue( createTxWithId( startTxId + 3 ) );
        txApplier.queue( createTxWithId( startTxId + 5 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 5 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 4 ) );
        txApplier.queue( createTxWithId( startTxId + 4 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 4 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 6 ) ); // ignored

        // when
        txApplier.applyBatch();

        // then
        assertTransactionsCommitted( startTxId + 1, 4 );
    }

    @Test
    void shouldBeAbleToQueueMaxBatchSize() throws Exception
    {
        // given
        var endTxId = startTxId + maxBatchSize;
        for ( var txId = startTxId + 1; txId <= endTxId; txId++ )
        {
            txApplier.queue( createTxWithId( txId ) );
        }

        // when
        txApplier.applyBatch();

        // then
        assertTransactionsCommitted( startTxId + 1, maxBatchSize );
    }

    @Test
    @Timeout( 3 )
    void shouldGiveUpQueueingOnStop() throws Throwable
    {
        // given
        for ( int i = 1; i <= maxBatchSize; i++ ) // fell the queue
        {
            txApplier.queue( createTxWithId( startTxId + i ) );
        }

        // when
        var latch = new CountDownLatch( 1 );
        var thread = new Thread( () -> {
            latch.countDown();
            try
            {
                txApplier.queue( createTxWithId( startTxId + maxBatchSize + 1 ) );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        } );

        thread.start();

        latch.await();
        txApplier.stop();

        // then we don't get stuck
        thread.join();
    }

    @Test
    void tracePageCacheAccessOnTransactionApply() throws Exception
    {
        txApplier.queue( createTxWithId( startTxId + 1 ) );
        txApplier.queue( createTxWithId( startTxId + 2 ) );
        txApplier.queue( createTxWithId( startTxId + 3 ) );

        txApplier.applyBatch();

        pageCacheTracer.checkAllCursorsAreClosed();
    }

    private CommittedTransactionRepresentation createTxWithId( long txId )
    {
        var tx = mock( CommittedTransactionRepresentation.class );
        var commitEntry = mock( LogEntryCommit.class );
        when( commitEntry.getTxId() ).thenReturn( txId );
        var txRep = mock( TransactionRepresentation.class );
        var encodedRaftLogIndex = encodeLogIndexAsTxHeader( txId - 5 ); // just some arbitrary offset
        when( txRep.additionalHeader() ).thenReturn( encodedRaftLogIndex );
        when( tx.getTransactionRepresentation() ).thenReturn( txRep );
        when( tx.getCommitEntry() ).thenReturn( commitEntry );
        return tx;
    }

    private void assertTransactionsCommitted( long startTxId, long expectedCount ) throws TransactionFailureException
    {
        var batchCaptor = ArgumentCaptor.forClass( TransactionToApply.class );
        verify( commitProcess ).commit( batchCaptor.capture(), eq( NULL ), eq( EXTERNAL ) );

        TransactionToApply batch = Iterables.single( batchCaptor.getAllValues() );
        var expectedTxId = startTxId;
        var count = 0;
        while ( batch != null )
        {
            assertEquals( expectedTxId, batch.transactionId() );
            expectedTxId++;
            batch = batch.next();
            count++;
        }
        assertEquals( expectedCount, count );
    }

    private static class TrackingPageCacheTracer extends DefaultPageCacheTracer
    {
        private final List<PageCursorTracer> cursorTracers = new ArrayList<>();

        @Override
        public PageCursorTracer createPageCursorTracer( String tag )
        {
            var cursorTracer = mock( PageCursorTracer.class );
            cursorTracers.add( cursorTracer );
            return cursorTracer;
        }

        void checkAllCursorsAreClosed()
        {
            assertFalse( cursorTracers.isEmpty() );
            for ( PageCursorTracer cursorTracer : cursorTracers )
            {
                verify( cursorTracer ).close();
            }
        }
    }
}
