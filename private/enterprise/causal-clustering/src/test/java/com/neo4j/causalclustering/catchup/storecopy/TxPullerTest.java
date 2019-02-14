/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.identity.StoreId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TxPullerTest
{
    private static final int MAX_FAILED_TX_PULL_REQUESTS = 5;
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private TransactionLogCatchUpWriter writer;
    private CatchupAddressProvider addressProvider;
    private TxPullClient client;
    private TxPuller executor;
    private LastTxTracker lastTxTracker;

    @BeforeEach
    void setUp()
    {
        lastTxTracker = new LastTxTracker();
        writer = mock( TransactionLogCatchUpWriter.class );
        when( writer.lastTx() ).thenAnswer( i -> lastTxTracker.getLastTx() );
        addressProvider = mock( CatchupAddressProvider.class );
        client = mock( TxPullClient.class );
        executor = new TxPuller( addressProvider, logProvider, new MaxCount() );
    }

    @Test
    void shouldThrowAfterConsecutiveRequestsWithoutProgression() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 1 );
        TxPullRequestContext context = getContext( requiredRange );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenReturn( new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 0 ) );

        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( client, times( MAX_FAILED_TX_PULL_REQUESTS ) ).pullTransactions( any(), any(), anyLong(), any() );
    }

    @Test
    void shouldSucceedIfRangeIsMet() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = getContext( requiredRange );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenAnswer( lastTxTracker.txPullAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 100 ) );

        executor.pullTransactions( context, writer, client );
    }

    @Test
    void shouldRetryUntilRangeIsMetAndSuccessfulResponse() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = getContext( requiredRange );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) )
                .thenAnswer( lastTxTracker.txAnswerWithoutProgress( CatchupResult.E_TRANSACTION_PRUNED ) )
                .thenAnswer( lastTxTracker.txPullAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) )
                .thenThrow( Exception.class )
                .thenAnswer( lastTxTracker.txPullAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) );
        executor.pullTransactions( context, writer, client );

        verify( client, atLeast( 4 ) ).pullTransactions( any(), any(), anyLong(), any() );
    }

    @Test
    void shouldUsePrimaryMethodInLastRequestAfterConstraintIsMet() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = getContext( requiredRange );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenAnswer( lastTxTracker.txPullAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) );
        executor.pullTransactions( context, writer, client );

        InOrder inOrder = inOrder( addressProvider );
        inOrder.verify( addressProvider, times( 2 ) ).secondary();
        inOrder.verify( addressProvider ).primary();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldUsePrimaryAddressIfLastTry() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = getContext( requiredRange );

        OngoingStubbing<TxStreamFinishedResponse> stubbing = when( client.pullTransactions( any(), any(), anyLong(), any() ) );
        for ( int i = 0; i < MAX_FAILED_TX_PULL_REQUESTS - 1; i++ )
        {
            stubbing = stubbing.thenAnswer( lastTxTracker.txAnswerWithoutProgress( CatchupResult.SUCCESS_END_OF_STREAM ) );
        }
        stubbing.thenAnswer( lastTxTracker.txPullAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) );
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
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        OngoingStubbing<TxStreamFinishedResponse> stubbing = when( client.pullTransactions( any(), any(), anyLong(), any() ) );
        for ( int i = 0; i < MAX_FAILED_TX_PULL_REQUESTS - 1; i++ )
        {
            stubbing = stubbing.thenAnswer( lastTxTracker.txAnswerWithoutProgress( CatchupResult.SUCCESS_END_OF_STREAM ) );
        }
        stubbing.thenAnswer( lastTxTracker.txPullAnswer( CatchupResult.SUCCESS_END_OF_STREAM, 50 ) );
        executor.pullTransactions( context, writer, client );

        verify( context ).fallbackStartId();
    }

    @ParameterizedTest
    @EnumSource( value = CatchupResult.class, mode = EnumSource.Mode.EXCLUDE, names = {"SUCCESS_END_OF_STREAM", "SUCCESS_END_OF_BATCH"} )
    void shouldImmediatelyMoveToLastAttemptIfNoProgressAndErrorResponse( CatchupResult result ) throws Exception
    {
        // All error responses are considered non transient

        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenAnswer( lastTxTracker.txAnswerWithoutProgress( result ) );
        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( client, times( 2 ) ).pullTransactions( any(), any(), anyLong(), any() );
        verify( addressProvider, times( 1 ) ).secondary();
        verify( addressProvider, times( 1 ) ).primary();
    }

    @Test
    void shouldImmediatelyMoveToLastAttemptIfNoProgressAndNotTransientException() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenThrow( RuntimeException.class );
        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( client, times( 2 ) ).pullTransactions( any(), any(), anyLong(), any() );
        verify( addressProvider, times( 1 ) ).secondary();
        verify( addressProvider, times( 1 ) ).primary();
    }

    @Test
    void shouldUseProgressConditionIfNoProgressAndConnectException() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenThrow( ConnectException.class );
        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( client, times( MAX_FAILED_TX_PULL_REQUESTS ) ).pullTransactions( any(), any(), anyLong(), any() );
        verify( addressProvider, times( MAX_FAILED_TX_PULL_REQUESTS - 1 ) ).secondary();
        verify( addressProvider, times( 1 ) ).primary();
    }

    @Test
    void shouldUseProgressConditionIfNoProgressAndCatchupResolutionException() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        when( addressProvider.primary() ).thenThrow( CatchupAddressResolutionException.class );
        when( addressProvider.secondary() ).thenThrow( CatchupAddressResolutionException.class );

        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( addressProvider, times( MAX_FAILED_TX_PULL_REQUESTS - 1 ) ).secondary();
        verify( addressProvider, times( 1 ) ).primary();
        verify( client, never() ).pullTransactions( any(), any(), anyLong(), any() );
    }

    class LastTxTracker
    {
        AtomicLong lastTx = new AtomicLong( -1 );

        Answer<TxStreamFinishedResponse> txAnswerWithoutProgress( CatchupResult result )
        {
            return txPullAnswer( result, 0 );
        }

        Answer<TxStreamFinishedResponse> txPullAnswer( CatchupResult result, long txIdProgress )
        {
            return invocation ->
            {
                lastTx.addAndGet( txIdProgress );
                return new TxStreamFinishedResponse( result, 0 );
            };
        }

        long getLastTx()
        {
            return lastTx.longValue();
        }
    }

    private TxPullRequestContext getContext( RequiredTransactions requiredRange )
    {
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        return TxPullRequestContext.createContextFromStoreCopy( requiredRange, storeId );
    }

    private static class MaxCount implements ResettableCondition
    {
        int count;

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
