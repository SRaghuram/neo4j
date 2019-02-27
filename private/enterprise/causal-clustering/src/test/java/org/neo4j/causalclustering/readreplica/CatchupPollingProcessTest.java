/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.neo4j.causalclustering.catchup.CatchupClientFactory;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.MockCatchupClient;
import org.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV1;
import org.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV2;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV1;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV2;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.common.StubLocalDatabaseService;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.causalclustering.helpers.FakeExecutor;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.STORE_COPYING;
import static org.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.TX_PULLING;
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
    private final CatchupClientV1 v1Client = spy( new MockClientV1( clientResponses ) );
    private final CatchupClientV2 v2Client = spy( new MockClientV2( clientResponses ) );
    private final Consumer<Throwable> globalPanic = ( Throwable e ) ->
    {
        //do nothing
    };

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
        when( catchupClientFactory.getClient( any( AdvertisedSocketAddress.class ), any( Log.class ) ) ).thenReturn( catchupClient );
        txPuller = new CatchupPollingProcess( executor, databaseName, databaseService, startStopOnStoreCopy, catchupClientFactory,
                strategyPipeline, txApplier, new Monitors(), storeCopy, topologyService, NullLogProvider.getInstance(), globalPanic );
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
        verify( v1Client, times( 1 ) ).pullTransactions( any( StoreId.class ), anyLong() );
        verify( v2Client, times( 1 ) ).pullTransactions( any( StoreId.class ), anyLong(), anyString() );
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
