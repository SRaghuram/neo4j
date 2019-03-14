/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.Suspendable;
import com.neo4j.causalclustering.helpers.FakeExecutor;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.neo4j.causalclustering.catchup.MockCatchupClient.responses;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.STORE_COPYING;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.TX_PULLING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

public class CatchupPollingProcessTest
{
    private final CatchupClientFactory catchupClientFactory = mock( CatchupClientFactory.class );
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );
    private final Executor executor = new FakeExecutor();
    private final BatchingTxApplier txApplier = mock( BatchingTxApplier.class );
    private final ClusteredDatabaseContext clusteredDatabaseContext = mock( ClusteredDatabaseContext.class );
    private final TopologyService topologyService = mock( TopologyService.class );
    private final StoreCopyProcess storeCopy = mock( StoreCopyProcess.class );
    private final Suspendable startStopOnStoreCopy = mock( Suspendable.class );
    private final StubClusteredDatabaseManager databaseService = spy( new StubClusteredDatabaseManager() );
    private final String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
    private final StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );
    private final AdvertisedSocketAddress coreMemberAddress = new AdvertisedSocketAddress( "hostname", 1234 );

    private final MockClientResponses clientResponses = responses();
    private final CatchupClientV3 v3Client = spy( new MockClientV3( clientResponses ) );
    private final Panicker panicker = mock( Panicker.class );
    private CatchupAddressProvider catchupAddressProvider = mock( CatchupAddressProvider.class );

    private CatchupPollingProcess txPuller;
    private MockCatchupClient catchupClient;

    @Before
    public void before() throws Throwable
    {
        databaseService.registerDatabase( new DatabaseId( databaseName ), clusteredDatabaseContext );
        when( idStore.getLastCommittedTransactionId() ).thenReturn( BASE_TX_ID + 1 );
        when( clusteredDatabaseContext.storeId() ).thenReturn( storeId );
        when( catchupAddressProvider.primary() ).thenReturn( coreMemberAddress );
        when( catchupAddressProvider.secondary() ).thenReturn( coreMemberAddress );

        catchupClient = new MockCatchupClient( ApplicationProtocols.CATCHUP_3, v3Client );
        when( catchupClientFactory.getClient( any( AdvertisedSocketAddress.class ), any( Log.class ) ) ).thenReturn( catchupClient );
        txPuller = new CatchupPollingProcess( executor, databaseName, databaseService, startStopOnStoreCopy, catchupClientFactory, txApplier, new Monitors(),
                storeCopy, FormattedLogProvider.toOutputStream( System.out ), panicker, catchupAddressProvider );
    }

    @Test
    public void shouldSendPullRequestOnTickForAllVersions() throws Exception
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
        verify( v3Client, times( 2 ) ).pullTransactions( storeId, lastAppliedTxId, databaseName );
        verify( catchupAddressProvider, times( 2 ) ).primary();
    }

    @Test
    public void nextStateShouldBeStoreCopyingIfRequestedTransactionHasBeenPrunedAwayV1() throws Exception
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
    public void nextStateShouldBeStoreCopyingIfRequestedTransactionHasBeenPrunedAwayV2() throws Exception
    {
        // given
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        txPuller.start();
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_3 );

        // when
        txPuller.tick().get();

        // then
        assertEquals( STORE_COPYING, txPuller.state() );
    }

    @Test
    public void shouldUseProvidedCatchupAddressProviderWhenStoreCopying() throws Exception
    {
        // given
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        txPuller.start();
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_3 );

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
    public void nextStateShouldBeTxPullingAfterASuccessfulStoreCopyV1() throws Throwable
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
        verify( databaseService ).stopForStoreCopy();
        verify( startStopOnStoreCopy ).disable();
        verify( storeCopy ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );
        verify( databaseService, atLeast( 1 ) ).start();
        verify( startStopOnStoreCopy ).enable();
        verify( txApplier ).refreshFromNewStore();

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }

    @Test
    public void nextStateShouldBeTxPullingAfterASuccessfulStoreCopyV2() throws Throwable
    {
        // given
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        txPuller.start();
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_3 );

        // when (tx pull)
        txPuller.tick().get();
        // when (store copy)
        txPuller.tick().get();

        // then
        verify( databaseService ).stopForStoreCopy();
        verify( startStopOnStoreCopy ).disable();
        verify( storeCopy ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );
        verify( databaseService ).start();
        verify( startStopOnStoreCopy ).enable();
        verify( txApplier ).refreshFromNewStore();

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }

    @Test
    public void shouldPanicOnException() throws ExecutionException, InterruptedException
    {
        when( txApplier.lastQueuedTxId() ).thenThrow( IllegalStateException.class );
        txPuller.start();
        txPuller.tick().get();

        verify( panicker ).panic( any() );
    }

    @Test
    public void shouldNotSignalOperationalUntilPullingV1() throws Throwable
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
    public void shouldNotSignalOperationalUntilPullingV2() throws Throwable
    {
        // given
        when( txApplier.lastQueuedTxId() ).thenReturn( BASE_TX_ID + 1 );
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_3 );

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

}
