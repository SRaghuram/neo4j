/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.stream.LongStream;

import org.neo4j.collection.Dependencies;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DescriptiveAvailabilityRequirement;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.store.StoreFileClosedException;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.catchup.CatchupResult.E_INVALID_REQUEST;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.AuthSubject.ANONYMOUS;
import static org.neo4j.kernel.impl.api.state.StubCursors.cursor;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@ExtendWith( LifeExtension.class )
class TxPullRequestHandlerTest
{
    private static final NamedDatabaseId NAMED_DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();
    private static final DatabaseId DATABASE_ID = NAMED_DATABASE_ID.databaseId();
    private final ChannelHandlerContext context = mock( ChannelHandlerContext.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private final StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );
    private final Database database = mock( Database.class );
    private final DatabaseAvailabilityGuard availabilityGuard = new DatabaseAvailabilityGuard(
            NAMED_DATABASE_ID,
            new FakeClock(),
            NullLog.getInstance(), 0,
            mock( CompositeDatabaseAvailabilityGuard.class ) );
    private final LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );

    private TxPullRequestHandler txPullRequestHandler;

    @Inject
    private LifeSupport lifeSupport;

    @BeforeEach
    void setUp()
    {
        var dependencies = mock( Dependencies.class );
        when( database.getDependencyResolver() ).thenReturn( dependencies );
        when( dependencies.resolveDependency( LogicalTransactionStore.class ) ).thenReturn( logicalTransactionStore );
        when( dependencies.resolveDependency( TransactionIdStore.class ) ).thenReturn( transactionIdStore );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        when( database.getDatabaseAvailabilityGuard() ).thenReturn( availabilityGuard );
        when( database.getMonitors() ).thenReturn( new Monitors() );
        when( database.getStoreId() ).thenReturn( storeId );
        var databaseLogService = new DatabaseLogService( NAMED_DATABASE_ID, new SimpleLogService( logProvider ) );
        when( database.getInternalLogProvider() ).thenReturn( databaseLogService.getInternalLogProvider() );
        final var protocol = new CatchupServerProtocol();
        txPullRequestHandler = new TxPullRequestHandler( protocol, database );
        lifeSupport.add( availabilityGuard );
    }

    @Test
    void shouldFailWithStoreUnavailableIfTxStoreHasClosed() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenThrow( StoreFileClosedException.class );
        var channelFuture = mock( ChannelFuture.class );
        when( context.writeAndFlush( any() ) ).thenReturn( channelFuture );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId, DATABASE_ID ) );

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
        var channelFuture = mock( ChannelFuture.class );
        when( context.writeAndFlush( any() ) ).thenReturn( channelFuture );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId, DATABASE_ID ) );

        // then
        verify( context ).writeAndFlush( isA( TransactionStream.class ) );
    }

    @Test
    void shouldRespondWithEndOfStreamIfThereAreNoTransactions() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 14L );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 14, storeId, DATABASE_ID ) );

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
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId, DATABASE_ID ) );

        // then
        verify( context, never() ).write( isA( TransactionStream.class ) );
        verify( context, never() ).writeAndFlush( isA( TransactionStream.class ) );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_TRANSACTION_PRUNED, -1L ) );
        assertThat( logProvider ).forClass( TxPullRequestHandler.class ).forLevel( INFO ).containsMessageWithArguments(
                "Failed to serve TxPullRequest for tx %d because the transaction does not exist. Last committed tx %d", 14L, 15L );
    }

    @Test
    void shouldNotStreamTxEntriesIfStoreIdMismatches() throws Exception
    {
        // given
        var serverStoreId = new StoreId( 1, 2, 3, 4, 5 );
        var clientStoreId = new StoreId( 6, 7, 8, 9, 10 );

        when( database.getStoreId() ).thenReturn( serverStoreId );

        final var protocol = new CatchupServerProtocol();
        var txPullRequestHandler =
                new TxPullRequestHandler( protocol, database );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, clientStoreId, DATABASE_ID ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_ID_MISMATCH, -1L ) );
        assertThat( logProvider ).forClass( TxPullRequestHandler.class ).forLevel( INFO )
                .containsMessageWithArguments( "Failed to serve TxPullRequest for tx %d and storeId %s because that storeId " +
                                "is different from this machine with %s", 2L, clientStoreId, serverStoreId );
    }

    @Test
    void shouldNotStreamTxsAndReportErrorIfTheLocalDatabaseIsNotAvailable() throws Exception
    {
        // given
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Test" ) );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        final var protocol = new CatchupServerProtocol();
        var txPullRequestHandler =
                new TxPullRequestHandler( protocol, database );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, storeId, DATABASE_ID ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_UNAVAILABLE, -1L ) );
        assertThat( logProvider ).forClass( TxPullRequestHandler.class ).forLevel( INFO ).containsMessageWithArguments(
                "Failed to serve TxPullRequest for tx %d because the local database is unavailable.", 2L );
    }

    @ParameterizedTest
    @ValueSource( longs = {Long.MIN_VALUE, BASE_TX_ID - 42, BASE_TX_ID - 2, BASE_TX_ID - 1} )
    void shouldRespondWithIllegalRequestWhenTransactionIdIsIncorrect( long incorrectTxId ) throws Exception
    {
        final var protocol = new CatchupServerProtocol();
        var txPullRequestHandler =
                new TxPullRequestHandler( protocol, database );

        var request = mock( TxPullRequest.class );
        when( request.previousTxId() ).thenReturn( incorrectTxId );

        txPullRequestHandler.channelRead0( context, request );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_INVALID_REQUEST, -1L ) );
    }

    @Test
    void shouldReturnStreamOfTransactionsForBaseTransactionId() throws Exception
    {
        var previousTxId = BASE_TX_ID;
        var firstTxIdInTxStream = previousTxId + 1;
        long lastCommittedTxId = 42;

        var transactions = LongStream.rangeClosed( firstTxIdInTxStream, lastCommittedTxId )
                .mapToObj( TxPullRequestHandlerTest::tx )
                .toArray( CommittedTransactionRepresentation[]::new );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( lastCommittedTxId );
        when( logicalTransactionStore.getTransactions( BASE_TX_ID + 1 ) ).thenReturn( txCursor( transactions ) );
        var channelFuture = mock( ChannelFuture.class );
        when( context.writeAndFlush( any() ) ).thenReturn( channelFuture );

        final var protocol = new CatchupServerProtocol();
        var txPullRequestHandler =
                new TxPullRequestHandler( protocol, database );

        txPullRequestHandler.channelRead0( context, new TxPullRequest( previousTxId, storeId, DATABASE_ID ) );

        var txStreamCaptor = ArgumentCaptor.forClass( TransactionStream.class );
        verify( context ).writeAndFlush( txStreamCaptor.capture() );
        verifyTransactionStream( txStreamCaptor.getValue(), transactions );
        verify( logicalTransactionStore ).getTransactions( firstTxIdInTxStream );
    }

    private void verifyTransactionStream( TransactionStream txStream, CommittedTransactionRepresentation[] expectedTransactions ) throws Exception
    {
        ByteBufAllocator allocator = new UnpooledByteBufAllocator( false );
        var isFirst = true;
        for ( var tx : expectedTransactions )
        {
            if ( isFirst )
            {
                var chunk1 = txStream.readChunk( allocator );
                assertEquals( ResponseMessageType.TX, chunk1 );
                isFirst = false;
            }

            var chunk2 = txStream.readChunk( allocator );
            assertEquals( new TxPullResponse( storeId, tx ), chunk2 );
        }
    }

    private static CommittedTransactionRepresentation tx( long id )
    {
        var tx = new PhysicalTransactionRepresentation( Collections.singletonList( new TestCommand() ) );
        tx.setHeader( new byte[0], 0, 0, 0, 0, ANONYMOUS );
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id - 1, 0, new byte[]{}, LogPosition.UNSPECIFIED ),
                tx, new LogEntryCommit( id, id, BASE_TX_CHECKSUM ) );
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
