/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.net.ConnectException;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.StoreId;

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
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class TxPullerTest
{
    private static final NamedDatabaseId DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();

    private static final int MAX_FAILED_TX_PULL_REQUESTS = 5;
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
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
        executor = new TxPuller( addressProvider, logProvider, new MaxCount(), Clock.systemUTC(), DATABASE_ID );
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
        inOrder.verify( addressProvider, times( 2 ) ).secondary( DATABASE_ID );
        inOrder.verify( addressProvider ).primary( DATABASE_ID );
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
        inOrder.verify( addressProvider, times( MAX_FAILED_TX_PULL_REQUESTS - 1 ) ).secondary( DATABASE_ID );
        // last change chooses primary
        inOrder.verify( addressProvider ).primary( DATABASE_ID );
        // since last was successful, go back to secondary
        inOrder.verify( addressProvider ).secondary( DATABASE_ID );
        // complete state, do one last on primary
        inOrder.verify( addressProvider ).primary( DATABASE_ID );
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
    @EnumSource( value = CatchupResult.class, mode = EnumSource.Mode.EXCLUDE, names = {"SUCCESS_END_OF_STREAM"} )
    void shouldImmediatelyMoveToLastAttemptIfNoProgressAndErrorResponse( CatchupResult result ) throws Exception
    {
        // All error responses are considered non transient

        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenAnswer( lastTxTracker.txAnswerWithoutProgress( result ) );
        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( client, times( 2 ) ).pullTransactions( any(), any(), anyLong(), any() );
        verify( addressProvider, times( 1 ) ).secondary( DATABASE_ID );
        verify( addressProvider, times( 1 ) ).primary( DATABASE_ID );
    }

    @Test
    void shouldImmediatelyMoveToLastAttemptIfNoProgressAndNotTransientException() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenThrow( RuntimeException.class );
        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( client, times( 2 ) ).pullTransactions( any(), any(), anyLong(), any() );
        verify( addressProvider, times( 1 ) ).secondary( DATABASE_ID );
        verify( addressProvider, times( 1 ) ).primary( DATABASE_ID );
    }

    @Test
    void shouldUseProgressConditionIfNoProgressAndConnectException() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenThrow( ConnectException.class );
        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( client, times( MAX_FAILED_TX_PULL_REQUESTS ) ).pullTransactions( any(), any(), anyLong(), any() );
        verify( addressProvider, times( MAX_FAILED_TX_PULL_REQUESTS - 1 ) ).secondary( DATABASE_ID );
        verify( addressProvider, times( 1 ) ).primary( DATABASE_ID );
    }

    @Test
    void shouldUseProgressConditionIfNoProgressAndCatchupResolutionException() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 99 );
        TxPullRequestContext context = spy( getContext( requiredRange ) );

        when( addressProvider.primary( DATABASE_ID ) ).thenThrow( CatchupAddressResolutionException.class );
        when( addressProvider.secondary( DATABASE_ID ) ).thenThrow( CatchupAddressResolutionException.class );

        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        verify( addressProvider, times( MAX_FAILED_TX_PULL_REQUESTS - 1 ) ).secondary( DATABASE_ID );
        verify( addressProvider, times( 1 ) ).primary( DATABASE_ID );
        verify( client, never() ).pullTransactions( any(), any(), anyLong(), any() );
    }

    private static TxPullRequestContext getContext( RequiredTransactions requiredRange )
    {
        StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );
        return TxPullRequestContext.createContextFromStoreCopy( requiredRange, storeId );
    }

    private static class LastTxTracker
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

    @Test
    void shouldThrowAfterPullTransactionsGenericException() throws Exception
    {
        RequiredTransactions requiredRange = RequiredTransactions.requiredRange( 0, 1 );
        TxPullRequestContext context = getContext( requiredRange );

        NullPointerException npe = new NullPointerException(  );

        when( client.pullTransactions( any(), any(), anyLong(), any() ) ).thenThrow( npe );

        assertThrows( StoreCopyFailedException.class, () -> executor.pullTransactions( context, writer, client ) );

        assertThat( logProvider ).forClass( TxPuller.class )
                .forLevel( WARN ).containsMessages( "Unexpected exception when pulling transactions" ).containsException(npe);
    }
}
