/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchUpClientException;
import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.configuration.CausalClusteringSettings;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.StorageEngineFactory.selectStorageEngine;

class RemoteStoreTest
{
    private static final NamedDatabaseId DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();

    private final StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );
    private final SocketAddress localhost = new SocketAddress( "127.0.0.1", 1234 );
    private final DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( new File( "destination" ) );
    private final CatchupAddressProvider catchupAddressProvider = new CatchupAddressProvider.SingleAddressProvider( localhost );
    private final TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

    @Test
    void shouldCopyStoreFilesAndPullTransactions() throws Exception
    {
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        TxPullClient txPullClient = mock( TxPullClient.class );
        when( storeCopyClient.copyStoreFiles( any(), any(), any(), any(), any() ) ).thenReturn( RequiredTransactions.noConstraint( 1 ) );
        when( txPullClient.pullTransactions( any(), any(), anyLong(), any() ) )
                .thenReturn( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, 13 ) );

        doStoreCopy( storeCopyClient, txPullClient, catchupAddressProvider, Config.defaults() );

        // then
        verify( storeCopyClient ).copyStoreFiles( eq( catchupAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(), any() );
        verify( txPullClient, atLeast( 1 ) ).pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() );
    }

    @Test
    void shouldSuccessfullyPullNoTxIfRangeAllowsIt() throws Exception
    {
        RequiredTransactions requiredTransactions = RequiredTransactions.noConstraint( 1 );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( catchupAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(),
                any() ) ).thenReturn( requiredTransactions );

        TxPullClient txPullClient = mock( TxPullClient.class );
        AtomicLong lastTxSupplier = new AtomicLong();
        when( txPullClient.pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() ) ).then( incrementTxIdResponse( lastTxSupplier, 0 ) );

        when( writer.lastTx() ).then( m -> lastTxSupplier.get() );

        doStoreCopy( storeCopyClient, txPullClient, catchupAddressProvider, Config.defaults() );

        // will call pull transactions twice. First to secondary and then finally towards primary.
        verify( txPullClient, times( 2 ) ).pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() );
    }

    @Test
    void shouldPullTxUntilConstraintRangeIsMet() throws Exception
    {
        RequiredTransactions requiredTransactions = RequiredTransactions.requiredRange( 1, 10 );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( catchupAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(),
                any() ) ).thenReturn( requiredTransactions );

        TxPullClient txPullClient = mock( TxPullClient.class );
        AtomicLong lastTxSupplier = new AtomicLong();
        when( txPullClient.pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() ) ).then( incrementTxIdResponse( lastTxSupplier, 1 ) );

        when( writer.lastTx() ).then( m -> lastTxSupplier.get() );

        doStoreCopy( storeCopyClient, txPullClient, catchupAddressProvider, Config.defaults() );

        verify( txPullClient, atLeast( 10 ) ).pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() );
    }

    @Test
    void shouldEventuallyFailPullingTxIfConstraintIsNotMet() throws Exception
    {
        RequiredTransactions requiredTransactions = RequiredTransactions.requiredRange( 1, 10 );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( catchupAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(),
                any() ) ).thenReturn( requiredTransactions );

        TxPullClient txPullClient = mock( TxPullClient.class );
        AtomicLong lastTxSupplier = new AtomicLong();
        when( txPullClient.pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() ) ).then( incrementTxIdResponse( lastTxSupplier, 0 ) );

        when( writer.lastTx() ).then( m -> lastTxSupplier.get() );

        StoreCopyFailedException copyFailedException = assertThrows( StoreCopyFailedException.class,
                () -> doStoreCopy( storeCopyClient, txPullClient, catchupAddressProvider,
                        Config.defaults( CausalClusteringSettings.catch_up_client_inactivity_timeout, Duration.ofSeconds( 0 ) ) ) );

        assertThat( copyFailedException.getMessage(), CoreMatchers.equalTo( "Pulling tx failed consecutively without progress" ) );
    }

    @Test
    void shouldSetLastPulledTransactionId() throws Exception
    {
        long lastFlushedTxId = 12;

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( catchupAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(),
                any() ) ).thenReturn( RequiredTransactions.noConstraint( lastFlushedTxId ) );

        TxPullClient txPullClient = mock( TxPullClient.class );
        when( txPullClient.pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() ) )
                .thenReturn( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, 13 ) );

        doStoreCopy( storeCopyClient, txPullClient, catchupAddressProvider, Config.defaults() );

        long previousTxId = lastFlushedTxId - 1; // the interface is defined as asking for the one preceding
        verify( txPullClient, atLeast( 1 ) ).pullTransactions( eq( localhost ), eq( storeId ), eq( previousTxId ), any() );
    }

    @Test
    void shouldCloseDownTxLogWriterIfTxStreamingFails() throws Exception
    {
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( any(), any(), any(), any(), any() ) ).thenReturn( RequiredTransactions.noConstraint( 1 ) );
        TxPullClient txPullClient = mock( TxPullClient.class );

        doThrow( CatchUpClientException.class ).when( txPullClient )
                .pullTransactions( isNull(), eq( storeId ), anyLong(), any() );

        assertThrows( StoreCopyFailedException.class, () -> doStoreCopy( storeCopyClient, txPullClient, catchupAddressProvider,
                Config.defaults( CausalClusteringSettings.catch_up_client_inactivity_timeout, Duration.ofSeconds( 0 ) ) ) );

        verify( writer ).close();
    }

    @Test
    void shouldCallCallPrimaryOnceInTheEnd() throws Exception
    {
        RequiredTransactions requiredTransactions = RequiredTransactions.requiredRange( 1, 3 );

        CatchupAddressProvider catchupAddressProvider = mock( CatchupAddressProvider.class );
        when( catchupAddressProvider.primary( DATABASE_ID ) ).thenReturn( localhost );
        when( catchupAddressProvider.secondary( DATABASE_ID ) ).thenReturn( localhost );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( catchupAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(), any() ) ).thenReturn(
                requiredTransactions );

        TxPullClient txPullClient = mock( TxPullClient.class );
        AtomicLong lastTxSupplier = new AtomicLong();
        // fail with progress still increments txId
        when( txPullClient.pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() ) ).then( incrementTxIdResponse( lastTxSupplier, 1 ) );

        when( writer.lastTx() ).then( m -> lastTxSupplier.get() );

        doStoreCopy( storeCopyClient, txPullClient, catchupAddressProvider, Config.defaults() );

        verify( catchupAddressProvider, times( 3 ) ).secondary( DATABASE_ID );
        verify( catchupAddressProvider, times( 1 ) ).primary( DATABASE_ID );
    }

    @Test
    void shouldCallPrimaryAddressIfFailingConsecutively() throws Exception
    {
        RequiredTransactions requiredTransactions = RequiredTransactions.requiredRange( 1, 3 );

        CatchupAddressProvider secondaryFailingAddressProvider = mock( CatchupAddressProvider.class );
        when( secondaryFailingAddressProvider.secondary( DATABASE_ID ) ).thenThrow( CatchupAddressResolutionException.class );
        when( secondaryFailingAddressProvider.primary( DATABASE_ID ) ).thenReturn( localhost );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( secondaryFailingAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(),
                any() ) ).thenReturn( requiredTransactions );

        TxPullClient txPullClient = mock( TxPullClient.class );
        AtomicLong lastTxSupplier = new AtomicLong();
        // fail with progress still increments txId
        when( txPullClient.pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() ) ).then( incrementTxIdResponse( lastTxSupplier, 1 ) );

        when( writer.lastTx() ).then( m -> lastTxSupplier.get() );

        doStoreCopy( storeCopyClient, txPullClient, secondaryFailingAddressProvider,
                Config.defaults( CausalClusteringSettings.catch_up_client_inactivity_timeout, Duration.ofSeconds( 0 ) ) );

        verify( secondaryFailingAddressProvider, atLeast( 1 ) ).primary( DATABASE_ID );
    }

    private void doStoreCopy( StoreCopyClient storeCopyClient, TxPullClient txPullClient, CatchupAddressProvider catchupAddressProvider, Config config )
            throws IOException, StoreCopyFailedException
    {
        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ), null,
                storeCopyClient, txPullClient, factory( writer ), config, new Monitors(), selectStorageEngine(), DATABASE_ID, PageCacheTracer.NULL, INSTANCE );

        remoteStore.copy( catchupAddressProvider, storeId, databaseLayout );
    }

    private Answer<TxStreamFinishedResponse> incrementTxIdResponse( AtomicLong lastTxSupplier, long incrementAmount )
    {
        return invocationOnMock ->
        {
            long txId = invocationOnMock.getArgument( 2 );
            long incrementedTxId = txId + incrementAmount;
            lastTxSupplier.set( incrementedTxId );
            return new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, incrementedTxId );
        };
    }

    private static TransactionLogCatchUpFactory factory( TransactionLogCatchUpWriter writer ) throws IOException
    {
        TransactionLogCatchUpFactory factory = mock( TransactionLogCatchUpFactory.class );
        when( factory.create( any(), any( FileSystemAbstraction.class ), isNull(), any( Config.class ), any( LogProvider.class ), any(),
                any(), anyBoolean(), anyBoolean(), any(), any() ) ).thenReturn( writer );
        return factory;
    }
}
