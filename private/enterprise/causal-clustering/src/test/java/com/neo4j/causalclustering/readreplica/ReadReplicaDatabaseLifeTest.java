/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.core.state.storage.InMemorySimpleStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAborter;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;

class ReadReplicaDatabaseLifeTest
{
    private final DatabaseId databaseA = TestDatabaseIdRepository.randomDatabaseId();
    private final RaftId raftId = RaftId.from( databaseA );
    private final MemberId memberA = new MemberId( UUID.randomUUID() );
    private final SocketAddress addressA = new SocketAddress( "127.0.0.1", 123 );
    private final StoreId storeA = new StoreId( 0, 1, 2, 3, 4 );
    private final StoreId storeB = new StoreId( 5, 6, 7, 8, 9 );

    TopologyService topologyService( DatabaseId databaseId, MemberId upstreamMember, SocketAddress address ) throws CatchupAddressResolutionException
    {
        TopologyService topologyService = mock( TopologyService.class );
        Map<MemberId,CoreServerInfo> members = Map.of( upstreamMember, mock( CoreServerInfo.class ) );
        when( topologyService.allCoreServers() ).thenReturn( members );
        when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( new DatabaseCoreTopology( databaseId, raftId, members ) );
        when( topologyService.findCatchupAddress( upstreamMember ) ).thenReturn( address );
        return topologyService;
    }

    private CatchupComponents catchupComponents( SocketAddress address, StoreId remoteStoreId ) throws StoreIdDownloadFailedException
    {
        RemoteStore remoteStore = mock( RemoteStore.class );
        when( remoteStore.getStoreId( address ) ).thenReturn( remoteStoreId );
        StoreCopyProcess storeCopyProcess = mock( StoreCopyProcess.class );
        return new CatchupComponents( remoteStore, storeCopyProcess );
    }

    private UpstreamDatabaseStrategySelector chooseFirstMember( TopologyService topologyService )
    {
        AlwaysChooseFirstMember firstMember = new AlwaysChooseFirstMember();
        firstMember.inject( topologyService, Config.defaults(), nullLogProvider(), null );
        return new UpstreamDatabaseStrategySelector( firstMember );
    }

    private DatabaseStartAborter neverAbort()
    {
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( DatabaseId.class ) ) ).thenReturn( false );
        return aborter;
    }

    private ReadReplicaDatabaseLife createReadReplicaDatabaseLife( TopologyService topologyService, CatchupComponents catchupComponents,
            ReadReplicaDatabaseContext localContext, Lifecycle catchupProcess )
    {
        return createReadReplicaDatabaseLife( topologyService, catchupComponents, localContext, catchupProcess, new InMemorySimpleStorage<>() );
    }

    private ReadReplicaDatabaseLife createReadReplicaDatabaseLife( TopologyService topologyService, CatchupComponents catchupComponents,
            ReadReplicaDatabaseContext localContext, Lifecycle catchupProcess, SimpleStorage<RaftId> raftIdStorage )
    {
        return new ReadReplicaDatabaseLife( localContext, catchupProcess, chooseFirstMember( topologyService ), nullLogProvider(),
                nullLogProvider(), topologyService, () -> catchupComponents, mock( LifeSupport.class ),
                new ClusterInternalDbmsOperator(), raftIdStorage, mock( PanicService.class ), neverAbort() );
    }

    private ReadReplicaDatabaseContext normalDatabase( DatabaseId databaseId, StoreId storeId, Boolean isEmpty ) throws IOException
    {
        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.readStoreId( any() ) ).thenReturn( storeId );
        when( storeFiles.isEmpty( any() ) ).thenReturn( isEmpty );

        Database kernelDatabase = mock( Database.class );
        when( kernelDatabase.getDatabaseId() ).thenReturn( databaseId );
        when( kernelDatabase.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );

        LogFiles txLogs = mock( LogFiles.class );
        return new ReadReplicaDatabaseContext( kernelDatabase, new Monitors(), new Dependencies(), storeFiles, txLogs, new ClusterInternalDbmsOperator() );
    }

    private ReadReplicaDatabaseContext failToReadLocalStoreId( DatabaseId databaseId, Class<? extends Throwable> throwableClass ) throws IOException
    {
        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.readStoreId( any() ) ).thenThrow( throwableClass );

        Database kernelDatabase = mock( Database.class );
        when( kernelDatabase.getDatabaseId() ).thenReturn( databaseId );
        when( kernelDatabase.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );

        LogFiles txLogs = mock( LogFiles.class );
        return new ReadReplicaDatabaseContext( kernelDatabase, new Monitors(), new Dependencies(), storeFiles, txLogs, new ClusterInternalDbmsOperator() );
    }

    @Test
    void shouldFailToStartOnRaftIdDatabaseIdMismatch() throws Throwable
    {
        // given
        var topologyService = topologyService( databaseA, memberA, addressA );
        var catchupComponents = catchupComponents( addressA, storeA );
        var catchupProcess = mock( Lifecycle.class );
        var databaseContext = normalDatabase( databaseA, storeA, false );

        var raftIdB = RaftIdFactory.random();
        var raftIdStorage = new InMemorySimpleStorage<RaftId>();
        raftIdStorage.writeState( raftIdB );

        var readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess, raftIdStorage );
        var exception = IllegalStateException.class;

        // when / then
        assertThrows( exception, readReplicaDatabaseLife::start );
        assertNeverStarted( databaseContext.database(), catchupProcess );
    }

    @Test
    void shouldFailToStartOnUnexpectedIssue() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( databaseA, memberA, addressA );
        CatchupComponents catchupComponents = catchupComponents( addressA, storeA );
        Lifecycle catchupProcess = mock( Lifecycle.class );

        Class<RuntimeException> exception = RuntimeException.class;
        ReadReplicaDatabaseContext databaseContext = failToReadLocalStoreId( databaseA, exception );

        ReadReplicaDatabaseLife readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );

        // when / then
        assertThrows( exception, readReplicaDatabaseLife::start );
        assertNeverStarted( databaseContext.database(), catchupProcess );
    }

    @Test
    void shouldRetryOnExpectedException() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( databaseA, memberA, addressA );

        CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        when( catchupComponents.remoteStore().getStoreId( addressA ) )
                .thenThrow( StoreIdDownloadFailedException.class )
                .thenReturn( storeA );

        Lifecycle catchupProcess = mock( Lifecycle.class );
        ReadReplicaDatabaseContext databaseContext = normalDatabase( databaseA, storeA, false );

        ReadReplicaDatabaseLife readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );

        // when
        readReplicaDatabaseLife.start();

        // when / then
        assertStartedNormally( databaseContext.database(), catchupProcess );
    }

    @Test
    void shouldReplaceEmptyMatchingStoreWithRemote() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( databaseA, memberA, addressA );
        CatchupComponents catchupComponents = catchupComponents( addressA, storeA );
        Lifecycle catchupProcess = mock( Lifecycle.class );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( databaseA, storeA, true );

        ReadReplicaDatabaseLife readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );

        // when
        readReplicaDatabaseLife.start();

        // then
        verify( catchupComponents.storeCopyProcess() ).replaceWithStoreFrom( any(), any() );
        assertStartedNormally( databaseContext.database(), catchupProcess );
    }

    @Test
    void shouldReplaceEmptyMismatchingStoreWithRemote() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( databaseA, memberA, addressA );
        CatchupComponents catchupComponents = catchupComponents( addressA, storeB );
        Lifecycle catchupProcess = mock( Lifecycle.class );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( databaseA, storeA, true );

        ReadReplicaDatabaseLife readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );

        // when
        readReplicaDatabaseLife.start();

        // then
        verify( catchupComponents.storeCopyProcess() ).replaceWithStoreFrom( any(), any() );
        assertStartedNormally( databaseContext.database(), catchupProcess );
    }

    @Test
    void shouldNotStartWithMismatchedNonEmptyStore() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( databaseA, memberA, addressA );
        CatchupComponents catchupComponents = catchupComponents( addressA, storeB );
        Lifecycle catchupProcess = mock( Lifecycle.class );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( databaseA, storeA, false );

        ReadReplicaDatabaseLife readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );

        // when / then
        RuntimeException ex = assertThrows( RuntimeException.class, readReplicaDatabaseLife::start );
        assertThat( ex.getMessage(),
                allOf( containsString( "This read replica cannot join the cluster." ), containsString( "is not empty and has a mismatching storeId" ) ) );

        assertNeverStarted( databaseContext.database(), catchupProcess );
    }

    @Test
    void shouldStartWithMatchingDatabase() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( databaseA, memberA, addressA );
        CatchupComponents catchupComponents = catchupComponents( addressA, storeA );
        Lifecycle catchupProcess = mock( Lifecycle.class );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( databaseA, storeA, false );

        ReadReplicaDatabaseLife readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );

        // when
        readReplicaDatabaseLife.start();

        // then
        assertStartedNormally( databaseContext.database(), catchupProcess );
    }

    @Test
    void shouldStopCatchingUpAndStopTheDatabaseOnStop() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( databaseA, memberA, addressA );
        CatchupComponents catchupComponents = catchupComponents( addressA, storeA );
        Lifecycle catchupProcess = mock( Lifecycle.class );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( databaseA, storeA, false );

        ReadReplicaDatabaseLife readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );
        readReplicaDatabaseLife.start();

        // when
        readReplicaDatabaseLife.stop();

        // then
        assertStoppedNormally( databaseContext.database(), catchupProcess );
    }

    @Test
    void shouldNotifyTopologyServiceOnStart() throws Exception
    {
        var topologyService = topologyService( databaseA, memberA, addressA );
        var catchupComponents = catchupComponents( addressA, storeA );
        var catchupProcess = mock( Lifecycle.class );

        var databaseContext = normalDatabase( databaseA, storeA, false );
        var readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );

        readReplicaDatabaseLife.start();

        verify( topologyService ).onDatabaseStart( databaseA );
    }

    @Test
    void shouldNotifyTopologyServiceOnStop() throws Exception
    {
        var topologyService = topologyService( databaseA, memberA, addressA );
        var catchupComponents = catchupComponents( addressA, storeA );
        var catchupProcess = mock( Lifecycle.class );

        var databaseContext = normalDatabase( databaseA, storeA, false );
        var readReplicaDatabaseLife = createReadReplicaDatabaseLife( topologyService, catchupComponents, databaseContext, catchupProcess );
        readReplicaDatabaseLife.start();

        readReplicaDatabaseLife.stop();

        var inOrder = inOrder( topologyService );
        inOrder.verify( topologyService ).onDatabaseStart( databaseA );
        inOrder.verify( topologyService ).onDatabaseStop( databaseA );
    }

    private void assertNeverStarted( Database database, Lifecycle catchupProcess ) throws Exception
    {
        verify( database, never() ).start();
        verify( catchupProcess, never() ).start();
    }

    private void assertStartedNormally( Database database, Lifecycle catchupProcess ) throws Exception
    {
        InOrder inOrder = inOrder( database, catchupProcess );

        inOrder.verify( database ).start();
        inOrder.verify( catchupProcess ).start();
    }

    private void assertStoppedNormally( Database database, Lifecycle catchupProcess ) throws Exception
    {
        InOrder inOrder = inOrder( database, catchupProcess );

        inOrder.verify( catchupProcess ).stop();
        inOrder.verify( database ).stop();
    }

    @ServiceProvider
    public static class AlwaysChooseFirstMember extends UpstreamDatabaseSelectionStrategy
    {
        AlwaysChooseFirstMember()
        {
            super( "always-choose-first-member" );
        }

        @Override
        public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
        {
            DatabaseCoreTopology coreTopology = topologyService.coreTopologyForDatabase( databaseId );
            return Optional.ofNullable( coreTopology.members().keySet().iterator().next() );
        }
    }
}
