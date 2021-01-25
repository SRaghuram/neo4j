/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.MockCatchupClient;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientResponses;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV3;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV3;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.error_handling.DatabasePanicEvent;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.CallingThreadExecutor;

import static com.neo4j.causalclustering.catchup.MockCatchupClient.responses;
import static com.neo4j.causalclustering.error_handling.DatabasePanicReason.CatchupFailed;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.STORE_COPYING;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.TX_PULLING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

class CatchupPollingProcessTest
{
    private final CatchupClientFactory catchupClientFactory = mock( CatchupClientFactory.class );
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );
    private final Executor executor = new CallingThreadExecutor();
    private final BatchingTxApplier txApplier = mock( BatchingTxApplier.class );
    private final ReplicatedDatabaseEventDispatch databaseEventDispatch = mock( ReplicatedDatabaseEventDispatch.class );
    private final ClusteredDatabaseContext clusteredDatabaseContext = mock( ClusteredDatabaseContext.class );
    private final StoreCopyProcess storeCopy = mock( StoreCopyProcess.class );
    private final ClusterInternalDbmsOperator.StoreCopyHandle storeCopyHandle = mock( ClusterInternalDbmsOperator.StoreCopyHandle.class );
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId namedDatabaseId = databaseIdRepository.defaultDatabase();
    private final StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );
    private final SocketAddress coreMemberAddress = new SocketAddress( "hostname", 1234 );

    private final MockClientResponses clientResponses = responses();
    private final CatchupClientV3 v3Client = spy( new MockClientV3( clientResponses, databaseIdRepository ) );
    private final Panicker panicker = mock( Panicker.class );
    private final CatchupAddressProvider catchupAddressProvider = mock( CatchupAddressProvider.class );
    private final Database kernelDatabase = mock( Database.class );

    private ReadReplicaDatabaseContext databaseContext;
    private CatchupPollingProcess txPuller;

    @BeforeEach
    void before() throws Throwable
    {
        when( kernelDatabase.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
        when( kernelDatabase.getStoreId() ).thenReturn( storeId );
        when( kernelDatabase.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );
        when( kernelDatabase.isStarted() ).thenReturn( true );

        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.readStoreId( any(), any() ) ).thenReturn( storeId );

        databaseContext = spy( new ReadReplicaDatabaseContext( kernelDatabase, new Monitors(), new Dependencies(), storeFiles, mock( LogFiles.class ),
                                                               new ClusterInternalDbmsOperator( NullLogProvider.nullLogProvider() ), PageCacheTracer.NULL ) );

        when( idStore.getLastCommittedTransactionId() ).thenReturn( BASE_TX_ID + 1 );
        when( clusteredDatabaseContext.storeId() ).thenReturn( storeId );
        when( catchupAddressProvider.primary( namedDatabaseId ) ).thenReturn( coreMemberAddress );
        when( catchupAddressProvider.secondary( namedDatabaseId ) ).thenReturn( coreMemberAddress );
        when( storeCopyHandle.release() ).thenReturn( true );
        doReturn( storeCopyHandle ).when( databaseContext ).stopForStoreCopy();
        clearInvocations( databaseContext );

        MockCatchupClient catchupClient = new MockCatchupClient( ApplicationProtocols.CATCHUP_3_0, v3Client );
        when( catchupClientFactory.getClient( any( SocketAddress.class ), any( Log.class ) ) ).thenReturn( catchupClient );
        txPuller = new CatchupPollingProcess( executor, databaseContext, catchupClientFactory, txApplier, databaseEventDispatch,
                storeCopy, nullLogProvider(), panicker, catchupAddressProvider );
    }

    @Test
    void shouldSendPullRequestOnTickForAllVersions() throws Exception
    {
        // given
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 10 ) );
        txPuller.start();
        long lastAppliedTxId = 99L;
        when( txApplier.lastQueuedTxId() ).thenReturn( lastAppliedTxId );
        // when
        txPuller.tick().get();
        txPuller.tick().get();

        // then
        verify( v3Client, times( 2 ) ).pullTransactions( storeId, lastAppliedTxId, namedDatabaseId );
        verify( catchupAddressProvider, times( 2 ) ).primary( namedDatabaseId );
    }

    @Test
    void nextStateShouldBeStoreCopyingIfRequestedTransactionHasBeenPrunedAway() throws Exception
    {
        // when
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        txPuller.start();

        // when
        txPuller.tick().get();

        // then
        assertEquals( STORE_COPYING, txPuller.state() );
    }

    @Test
    void shouldUseProvidedCatchupAddressProviderWhenStoreCopying() throws Exception
    {
        // given
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        txPuller.start();

        // when
        txPuller.tick().get();

        // then
        assertEquals( STORE_COPYING, txPuller.state() );

        // when
        txPuller.tick().get();

        // then
        verify( storeCopy ).replaceWithStoreFrom( catchupAddressProvider, storeId );
    }

    @Test
    void nextStateShouldBeTxPullingAfterASuccessfulStoreCopy() throws Throwable
    {
        // given
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        txPuller.start();

        // when (tx pull)
        txPuller.tick().get();
        // when (store copy)
        txPuller.tick().get();

        // then
        verify( databaseContext ).stopForStoreCopy();
        verify( storeCopy ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );
        verify( storeCopyHandle ).release();
        verify( txApplier ).refreshFromNewStore();
        verify( databaseEventDispatch ).fireStoreReplaced( BASE_TX_ID + 1 );

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }

    @Test
    void shouldPanicOnException() throws ExecutionException, InterruptedException
    {
        when( txApplier.lastQueuedTxId() ).thenThrow( IllegalStateException.class );
        txPuller.start();
        txPuller.tick().get();

        verify( panicker ).panic( new DatabasePanicEvent( namedDatabaseId, CatchupFailed, any() ) );
    }

    @Test
    void shouldNotSignalOperationalUntilPulling() throws Throwable
    {
        // given
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );

        // when
        txPuller.start();
        Future<Boolean> operationalFuture = txPuller.upToDateFuture();
        assertFalse( operationalFuture.isDone() );

        txPuller.tick().get(); // realises we need a store copy
        assertFalse( operationalFuture.isDone() );

        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 15 ) );

        txPuller.tick().get(); // does the store copy
        assertFalse( operationalFuture.isDone() );

        txPuller.tick().get(); // does a pulling
        assertTrue( operationalFuture.isDone() );
        assertTrue( operationalFuture.get() );

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }

    @Test
    void shouldAllowToRetryOnStoreCopyFailures() throws Throwable
    {
        // given
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        txPuller.start();

        // and store copy fails
        var storeCopyError = new StoreCopyFailedException( "" );
        doThrow( storeCopyError ).when( storeCopy ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );

        // when tx pull
        txPuller.tick().get();
        // when store copy #1
        txPuller.tick().get();

        // then
        verify( databaseContext ).stopForStoreCopy();
        verify( storeCopy ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );

        verify( storeCopyHandle, never() ).release();
        verify( txApplier, never() ).refreshFromNewStore();
        verify( databaseEventDispatch, never() ).fireStoreReplaced( BASE_TX_ID + 1 );

        // puller remains in store copying state after a failed store copy
        assertEquals( STORE_COPYING, txPuller.state() );

        // when store copy #2
        doNothing().when( storeCopy ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );
        txPuller.tick().get();

        // then
        verify( databaseContext ).stopForStoreCopy(); // only once
        verify( storeCopy, times( 2 ) ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );

        verify( storeCopyHandle ).release();
        verify( txApplier ).refreshFromNewStore();
        verify( databaseEventDispatch ).fireStoreReplaced( BASE_TX_ID + 1 );

        // puller moves to tx pulling after a successful store copy
        assertEquals( TX_PULLING, txPuller.state() );
    }

    @Test
    void shouldAllowToRetryOnKernelStartFailure() throws Throwable
    {
        // given:
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        txPuller.start();

        // given: kernel failing to start
        when( kernelDatabase.isStarted() ).thenReturn( false );

        // when tx pull, moves over to store copy state
        txPuller.tick().get();
        // when store copy attempt #1
        txPuller.tick().get();

        // then
        verify( databaseContext ).stopForStoreCopy();
        verify( storeCopy ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );

        verify( storeCopyHandle ).release();
        verify( txApplier, never() ).refreshFromNewStore();
        verify( databaseEventDispatch, never() ).fireStoreReplaced( BASE_TX_ID + 1 );

        // puller remains in store copying state after a failed store copy
        assertEquals( STORE_COPYING, txPuller.state() );

        // store copy attempt #2 with the kernel now starting correctly
        when( kernelDatabase.isStarted() ).thenReturn( true );
        txPuller.tick().get();

        // then
        verify( databaseContext, times( 2 ) ).stopForStoreCopy();
        verify( storeCopy, times( 2 ) ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );

        verify( storeCopyHandle, times( 2 ) ).release();
        verify( txApplier ).refreshFromNewStore();
        verify( databaseEventDispatch ).fireStoreReplaced( BASE_TX_ID + 1 );

        // puller moves to tx pulling after a successful store copy
        assertEquals( TX_PULLING, txPuller.state() );
    }
}
