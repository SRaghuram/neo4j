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
import com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV1;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV2;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.common.LocalDatabase;
import com.neo4j.causalclustering.common.StubLocalDatabaseService;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.Suspendable;
import com.neo4j.causalclustering.helpers.FakeExecutor;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.STORE_COPYING;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.TX_PULLING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class CatchupPollingProcessTest
{
    private final UpstreamDatabaseStrategySelector strategyPipeline = mock( UpstreamDatabaseStrategySelector.class );
    private final MemberId coreMemberId = mock( MemberId.class );
    private final CatchupClientFactory catchupClientFactory = mock( CatchupClientFactory.class );
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );
    private final Executor executor = new FakeExecutor();
    private final BatchingTxApplier txApplier = mock( BatchingTxApplier.class );
    private final LocalDatabase localDatabase = mock( LocalDatabase.class );
    private final TopologyService topologyService = mock( TopologyService.class );
    private final StoreCopyProcess storeCopy = mock( StoreCopyProcess.class );
    private final Suspendable startStopOnStoreCopy = mock( Suspendable.class );
    private final StubLocalDatabaseService databaseService = spy( new StubLocalDatabaseService() );
    private final String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final AdvertisedSocketAddress coreMemberAddress = new AdvertisedSocketAddress( "hostname", 1234 );

    private final MockCatchupClient.MockClientResponses clientResponses = MockCatchupClient.responses();
    private final CatchupClientV1 v1Client = Mockito.spy( new MockCatchupClient.MockClientV1( clientResponses ) );
    private final CatchupClientV2 v2Client = Mockito.spy( new MockCatchupClient.MockClientV2( clientResponses ) );
    private final Panicker panicker = mock( Panicker.class );

    private CatchupPollingProcess txPuller;
    private MockCatchupClient catchupClient;

    @Before
    public void before() throws Throwable
    {
        databaseService.registerDatabase( databaseName, localDatabase );
        when( idStore.getLastCommittedTransactionId() ).thenReturn( BASE_TX_ID + 1 );
        when( strategyPipeline.bestUpstreamDatabase() ).thenReturn( coreMemberId );
        when( localDatabase.storeId() ).thenReturn( storeId );
        when( topologyService.findCatchupAddress( coreMemberId ) ).thenReturn( coreMemberAddress );

        catchupClient = new MockCatchupClient( ApplicationProtocols.CATCHUP_1, v1Client, v2Client );
        when( catchupClientFactory.getClient( any( AdvertisedSocketAddress.class ) ) ).thenReturn( catchupClient );
        txPuller = new CatchupPollingProcess( executor, databaseName, databaseService, startStopOnStoreCopy, catchupClientFactory,
                strategyPipeline, txApplier, new Monitors(), storeCopy, topologyService, NullLogProvider.getInstance(), panicker );
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
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_2 ); // Need to switch protocols to test both clients without having two txPuller objects
        txPuller.tick().get();

        // then
        verify( v1Client ).pullTransactions( storeId, lastAppliedTxId );
        verify( v2Client ).pullTransactions( storeId, lastAppliedTxId, databaseName );
    }

    @Test
    public void shouldKeepMakingPullRequestsUntilEndOfStream() throws Exception
    {
        //TODO make a real test
        // given
        clientResponses.withTxPullResponse( new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 10 ) );
        txPuller.start();
        long lastAppliedTxId = 99L;
        when( txApplier.lastQueuedTxId() ).thenReturn( lastAppliedTxId );

        // when
        txPuller.tick().get();
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_2 );
        txPuller.tick().get();

        // then
        verify( v1Client ).pullTransactions( any( StoreId.class ), anyLong() );
        verify( v2Client ).pullTransactions( any( StoreId.class ), anyLong(), anyString() );
    }

    // TODO:  Come up with a way of avoiding V1/V2 versions of these tests.  I tried to parameterize instead but that wasn't much better re: clarity.
    // I believe it *should* be possible with a bunch of unsafe casting, but again, messy and confusing. The problem is that we need to expect/verify
    // different method calls (pullTransactions with 2 or 3 params) for each catchup version. Open to ideas ...
    // Should be able to simple parameterize with the new mock catchup client

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
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_2 );

        // when
        txPuller.tick().get();

        // then
        assertEquals( STORE_COPYING, txPuller.state() );
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
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_2 );

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
        catchupClient.setProtocol( ApplicationProtocols.CATCHUP_2 );

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
