/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import com.neo4j.causalclustering.identity.StoreId;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.stream.LongStream;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DescriptiveAvailabilityRequirement;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.store.StoreFileClosedException;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.catchup.CatchupResult.E_INVALID_REQUEST;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static java.lang.Math.toIntExact;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.api.state.StubCursors.cursor;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class TxPullRequestHandlerTest
{
    private final ChannelHandlerContext context = mock( ChannelHandlerContext.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private Database database = mock( Database.class );
    private DatabaseAvailabilityGuard availabilityGuard = new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, new FakeClock(), NullLog.getInstance() );
    private LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
    private TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );

    private TxPullRequestHandler txPullRequestHandler;

    @BeforeEach
    void setUp()
    {
        Dependencies dependencies = mock( Dependencies.class );
        when( database.getDependencyResolver() ).thenReturn( dependencies );
        when( dependencies.resolveDependency( LogicalTransactionStore.class ) ).thenReturn( logicalTransactionStore );
        when( dependencies.resolveDependency( TransactionIdStore.class ) ).thenReturn( transactionIdStore );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        when( database.getDatabaseAvailabilityGuard() ).thenReturn( availabilityGuard );
        when( database.getMonitors() ).thenReturn( new Monitors() );
        when( database.getStoreId() ).thenReturn( toKernelStoreId( storeId ) );
        txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(), database, logProvider );
    }

    @Test
    void shouldFailWithStoreUnavailableIfTxStoreHasClosed() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenThrow( StoreFileClosedException.class );
        ChannelFuture channelFuture = mock( ChannelFuture.class );
        when( context.writeAndFlush( any() ) ).thenReturn( channelFuture );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId, DEFAULT_DATABASE_NAME ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_UNAVAILABLE, -1L ) );
    }

    @Test
    void shouldRespondWithCompleteStreamOfTransactions() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        when( logicalTransactionStore.getTransactions( 14L ) ).thenReturn( txCursor( tx( 14 ), tx( 15 ) ) );
        ChannelFuture channelFuture = mock( ChannelFuture.class );
        when( context.writeAndFlush( any() ) ).thenReturn( channelFuture );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId, DEFAULT_DATABASE_NAME ) );

        // then
        verify( context ).writeAndFlush( isA( ChunkedTransactionStream.class ) );
    }

    @Test
    void shouldRespondWithEndOfStreamIfThereAreNoTransactions() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 14L );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 14, storeId, DEFAULT_DATABASE_NAME ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, 14L ) );
    }

    @Test
    void shouldRespondWithoutTransactionsIfTheyDoNotExist() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        when( logicalTransactionStore.getTransactions( 14L ) ).thenThrow( new NoSuchTransactionException( 14 ) );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId, DEFAULT_DATABASE_NAME ) );

        // then
        verify( context, never() ).write( isA( ChunkedTransactionStream.class ) );
        verify( context, never() ).writeAndFlush( isA( ChunkedTransactionStream.class ) );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_TRANSACTION_PRUNED, -1L ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d because the transaction does not exist.", 14L ) );
    }

    @Test
    void shouldNotStreamTxEntriesIfStoreIdMismatches() throws Exception
    {
        // given
        StoreId serverStoreId = new StoreId( 1, 2, 3, 4 );
        StoreId clientStoreId = new StoreId( 5, 6, 7, 8 );

        when( database.getStoreId() ).thenReturn( toKernelStoreId( serverStoreId ) );

        TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(), database, logProvider );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, clientStoreId, DEFAULT_DATABASE_NAME ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_ID_MISMATCH, -1L ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d and storeId %s because that storeId is different " +
                        "from this machine with %s", 2L, clientStoreId, serverStoreId ) );
    }

    @Test
    void shouldNotStreamTxsAndReportErrorIfTheLocalDatabaseIsNotAvailable() throws Exception
    {
        // given
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Test" ) );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(), database, logProvider );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, storeId, DEFAULT_DATABASE_NAME ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_UNAVAILABLE, -1L ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d because the local database is unavailable.", 2L ) );
    }

    @ParameterizedTest
    @ValueSource( longs = {Long.MIN_VALUE, BASE_TX_ID - 42, BASE_TX_ID - 2, BASE_TX_ID - 1} )
    void shouldRespondWithIllegalRequestWhenTransactionIdIsIncorrect( long incorrectTxId ) throws Exception
    {
        TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(), database, logProvider );

        TxPullRequest request = mock( TxPullRequest.class );
        when( request.previousTxId() ).thenReturn( incorrectTxId );

        txPullRequestHandler.channelRead0( context, request );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_INVALID_REQUEST, -1L ) );
    }

    @Test
    void shouldReturnStreamOfTransactionsForBaseTransactionId() throws Exception
    {
        long previousTxId = BASE_TX_ID;
        long firstTxIdInTxStream = previousTxId + 1;
        long lastCommittedTxId = 42;

        CommittedTransactionRepresentation[] transactions = LongStream.rangeClosed( firstTxIdInTxStream, lastCommittedTxId )
                .mapToObj( TxPullRequestHandlerTest::tx )
                .toArray( CommittedTransactionRepresentation[]::new );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( lastCommittedTxId );
        when( logicalTransactionStore.getTransactions( BASE_TX_ID + 1 ) ).thenReturn( txCursor( transactions ) );
        ChannelFuture channelFuture = mock( ChannelFuture.class );
        when( context.writeAndFlush( any() ) ).thenReturn( channelFuture );

        TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(), database, logProvider );

        txPullRequestHandler.channelRead0( context, new TxPullRequest( previousTxId, storeId, DEFAULT_DATABASE_NAME ) );

        ArgumentCaptor<ChunkedTransactionStream> txStreamCaptor = ArgumentCaptor.forClass( ChunkedTransactionStream.class );
        verify( context ).writeAndFlush( txStreamCaptor.capture() );
        verifyTransactionStream( txStreamCaptor.getValue(), transactions );
        verify( logicalTransactionStore ).getTransactions( firstTxIdInTxStream );
    }

    private void verifyTransactionStream( ChunkedTransactionStream txStream, CommittedTransactionRepresentation[] expectedTransactions ) throws Exception
    {
        ByteBufAllocator allocator = new UnpooledByteBufAllocator( false );
        for ( CommittedTransactionRepresentation tx : expectedTransactions )
        {
            Object chunk1 = txStream.readChunk( allocator );
            assertEquals( ResponseMessageType.TX, chunk1 );

            Object chunk2 = txStream.readChunk( allocator );
            assertEquals( new TxPullResponse( storeId, tx ), chunk2 );
        }
    }

    private static CommittedTransactionRepresentation tx( long id )
    {
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.singletonList( new TestCommand() ) );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return new CommittedTransactionRepresentation(
                new LogEntryStart( toIntExact( id ), toIntExact( id ), id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                tx, new LogEntryCommit( id, id ) );
    }

    private static org.neo4j.storageengine.api.StoreId toKernelStoreId( StoreId storeId )
    {
        return new org.neo4j.storageengine.api.StoreId( storeId.getCreationTime(), storeId.getRandomId(), -1, storeId.getUpgradeTime(),
                storeId.getUpgradeId() );
    }

    private static TransactionCursor txCursor( CommittedTransactionRepresentation... transactions )
    {
        return new TransactionCursor()
        {
            final Cursor<CommittedTransactionRepresentation> cursor = cursor( transactions );

            @Override
            public LogPosition position()
            {
                throw new UnsupportedOperationException(
                        "LogPosition does not apply when moving a generic cursor over a list of transactions" );
            }

            @Override
            public boolean next()
            {
                return cursor.next();
            }

            @Override
            public void close()
            {
                cursor.close();
            }

            @Override
            public CommittedTransactionRepresentation get()
            {
                return cursor.get();
            }
        };
    }
}
