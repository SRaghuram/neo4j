/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.identity.StoreId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TxPullRequestExecutorTest
{
    private static final int MAX_FAILED_TX_PULL_REQUESTS = 5;
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private TransactionLogCatchUpWriter writer;
    private CatchupAddressProvider addressProvider;
    private TxPullClient client;
    private TxPullRequestExecutor executor;
    private AtomicLong lastTxTracker;

    @BeforeEach
    void setUp()
    {
        lastTxTracker = new AtomicLong(-1);
        writer = mock( TransactionLogCatchUpWriter.class );
        when( writer.lastTx() ).thenAnswer( i -> lastTxTracker.longValue() );
        addressProvider = mock( CatchupAddressProvider.class );
        client = mock( TxPullClient.class );
        executor = new TxPullRequestExecutor( addressProvider, logProvider, new MaxCount() );
    }

    @Test
    void shouldThrowAfterConsecutiveFailures() throws Exception
    {
        RequiredTransactionRange requiredRange = RequiredTransactionRange.range( 0, 1 );
        TxPullRequestContext context = getContext( requiredRange );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenReturn( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );

        Assertions.assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( client, times( MAX_FAILED_TX_PULL_REQUESTS ) ).pullTransactions( any(), any(), anyLong(), any() );
    }

    @Test
    void shouldSucceedIfRangeIsMet() throws Exception
    {
        RequiredTransactionRange requiredRange = RequiredTransactionRange.range( 0, 99 );
        TxPullRequestContext context = getContext( requiredRange );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenAnswer( txCompleteAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 100 ) );

        executor.pullTransactions( context, writer, client );
    }

    @Test
    void shouldRetryUntilRangeIsMetAndSuccessfulResponse() throws Exception
    {
        RequiredTransactionRange requiredRange = RequiredTransactionRange.range( 0, 99 );
        TxPullRequestContext context = getContext( requiredRange );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) )
                .thenAnswer( txCompleteAnswer( CatchupResult.E_TRANSACTION_PRUNED, 0 ) )
                .thenThrow( Exception.class )
                .thenAnswer( txCompleteAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) );
        executor.pullTransactions( context, writer, client );

        verify( client, atLeast( 4 ) ).pullTransactions( any(), any(), anyLong(), any() );
    }

    @Test
    void shouldUsePrimaryMethodInLastRequestAfterConstraintIsmet() throws Exception
    {
        RequiredTransactionRange requiredRange = RequiredTransactionRange.range( 0, 99 );
        TxPullRequestContext context = getContext( requiredRange );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenAnswer( txCompleteAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) );
        executor.pullTransactions( context, writer, client );

        InOrder inOrder = inOrder( addressProvider );
        inOrder.verify( addressProvider, times( 2 ) ).secondary();
        inOrder.verify( addressProvider ).primary();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldUsePrimaryAddressIfLastTry() throws Exception
    {
        RequiredTransactionRange requiredRange = RequiredTransactionRange.range( 0, 99 );
        TxPullRequestContext context = getContext( requiredRange );

        OngoingStubbing<TxStreamFinishedResponse> stubbing = when( client.pullTransactions( any(), any(), anyLong(), any() ) );
        for ( int i = 0; i < MAX_FAILED_TX_PULL_REQUESTS - 1; i++ )
        {
            stubbing = stubbing.thenAnswer( txCompleteAnswer( CatchupResult.E_INVALID_REQUEST, 0 ) );
        }
        stubbing.thenAnswer( txCompleteAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) );
        executor.pullTransactions( context, writer, client );

        InOrder inOrder = inOrder( addressProvider );
        // choose secondary until one change left
        inOrder.verify( addressProvider, times( MAX_FAILED_TX_PULL_REQUESTS - 1 ) ).secondary();
        // last change chooses primary
        inOrder.verify( addressProvider ).primary();
        // since last was successful, go back to secondary
        inOrder.verify( addressProvider ).secondary();
        // complete state, do one last on primary
        inOrder.verify( addressProvider ).primary();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldUseFallbackStartTxIdIfLastAttempt() throws Exception
    {
        RequiredTransactionRange requiredRange = RequiredTransactionRange.range( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        OngoingStubbing<TxStreamFinishedResponse> stubbing = when( client.pullTransactions( any(), any(), anyLong(), any() ) );
        for ( int i = 0; i < MAX_FAILED_TX_PULL_REQUESTS - 1; i++ )
        {
            stubbing = stubbing.thenAnswer( txCompleteAnswer( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        }
        stubbing.thenAnswer( txCompleteAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) );
        executor.pullTransactions( context, writer, client );

        verify( context ).fallbackStartId();
    }

    private Answer<TxStreamFinishedResponse> txCompleteAnswer( CatchupResult result, long incrementHighestTx )
    {
        return invocation ->
        {
            lastTxTracker.addAndGet( incrementHighestTx );
            return new TxStreamFinishedResponse( result, 0 );
        };
    }

    private TxPullRequestContext getContext( RequiredTransactionRange requiredRange )
    {
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        return TxPullRequestContext.createContextFromStoreCopy( requiredRange, storeId );
    }

    private static class MaxCount implements ResettableCondition
    {
        int count = 0;

        @Override
        public boolean canContinue()
        {
            return ++count < (MAX_FAILED_TX_PULL_REQUESTS - 1);
        }

        @Override
        public void reset()
        {
            count = 0;
        }
    }
}
