/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAborter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;

class ReadReplicaBootstrapTest
{
    private final NamedDatabaseId namedDatabaseA = TestDatabaseIdRepository.randomNamedDatabaseId();
    private final RaftId raftId = RaftId.from( namedDatabaseA.databaseId() );
    private final MemberId memberA = new MemberId( UUID.randomUUID() );
    private final SocketAddress addressA = new SocketAddress( "127.0.0.1", 123 );
    private final StoreId storeA = new StoreId( 0, 1, 2, 3, 4 );
    private final StoreId storeB = new StoreId( 5, 6, 7, 8, 9 );

    private ReadReplicaBootstrap createBootstrap( TopologyService topologyService, CatchupComponentsRepository.CatchupComponents catchupComponents,
            ReadReplicaDatabaseContext databaseContext, DatabaseStartAborter aborter )
    {
        return new ReadReplicaBootstrap( databaseContext, chooseFirstMember( topologyService ), nullLogProvider(), nullLogProvider(), topologyService,
                () -> catchupComponents, new ClusterInternalDbmsOperator(), aborter );
    }

    @Test
    void shouldFailToStartWhenStartAborted() throws Throwable
    {
        // given
        var topologyService = topologyService( namedDatabaseA, memberA, addressA );

        // Catchup components should throw an expected exception to cause a retry
        var catchupComponents = catchupComponents( addressA, storeA );
        when( catchupComponents.remoteStore().getStoreId( addressA ) )
                .thenThrow( StoreIdDownloadFailedException.class )
                .thenReturn( storeA );

        var databaseContext = normalDatabase( namedDatabaseA, storeA, false );

        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( false, true );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, aborter );

        // when / then
        assertThrows( DatabaseStartAbortedException.class, bootstrap::perform );
        verify( aborter, times( 2 ) ).shouldAbort( namedDatabaseA );
    }

    @Test
    void shouldClearAborterCacheOnStart() throws Throwable
    {
        // given
        var topologyService = topologyService( namedDatabaseA, memberA, addressA );
        var catchupComponents = catchupComponents( addressA, storeA );
        var databaseContext = normalDatabase( namedDatabaseA, storeA, false );
        var aborter = neverAbort();

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, aborter );

        // when
        bootstrap.perform();

        // then
        verify( aborter ).started( namedDatabaseA );
    }

    @Test
    void shouldClearAborterCacheOnFailedStart() throws Throwable
    {
        // given
        var topologyService = topologyService( namedDatabaseA, memberA, addressA );
        var catchupComponents = catchupComponents( addressA, storeA );

        var exception = RuntimeException.class;
        var databaseContext = failToReadLocalStoreId( namedDatabaseA, exception );
        var aborter = neverAbort();

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, aborter );

        // when / then
        assertThrows( exception, bootstrap::perform );
        verify( aborter ).started( namedDatabaseA );
    }

    @Test
    void shouldRetryOnExpectedException() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( namedDatabaseA, memberA, addressA );

        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        when( catchupComponents.remoteStore().getStoreId( addressA ) )
                .thenThrow( StoreIdDownloadFailedException.class )
                .thenReturn( storeA );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, storeA, false );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort() );

        // when
        bootstrap.perform(); // should not throw

        // then
        verify( catchupComponents.remoteStore(), times( 2 ) ).getStoreId( addressA );
    }

    @Test
    void shouldReplaceEmptyMismatchingStoreWithRemote() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( namedDatabaseA, memberA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeB );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, storeA, true );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort() );

        // when
        bootstrap.perform();

        // then
        verify( catchupComponents.storeCopyProcess() ).replaceWithStoreFrom( any(), any() );
    }

    @Test
    void shouldNotStartWithMismatchedNonEmptyStore() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( namedDatabaseA, memberA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeB );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, storeA, false );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort() );

        // when / then
        RuntimeException ex = assertThrows( RuntimeException.class, bootstrap::perform );
        assertThat( ex.getMessage(), allOf(
                containsString( "This read replica cannot join the cluster." ),
                containsString( "is not empty and has a mismatching storeId" ) ) );
    }

    @Test
    void shouldStartWithMatchingDatabase() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( namedDatabaseA, memberA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, storeA, false );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort() );

        // when / then
        bootstrap.perform();

        // then
        verify( catchupComponents.remoteStore(), times( 1 ) ).getStoreId( addressA );
    }

    @Test
    void shouldReplaceEmptyMatchingStoreWithRemote() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( namedDatabaseA, memberA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, storeA, true );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort() );

        // when
        bootstrap.perform();

        // then
        verify( catchupComponents.storeCopyProcess() ).replaceWithStoreFrom( any(), any() );
    }

    @Test
    void shouldFailToStartOnUnexpectedIssue() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( namedDatabaseA, memberA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        Class<RuntimeException> exception = RuntimeException.class;
        ReadReplicaDatabaseContext databaseContext = failToReadLocalStoreId( namedDatabaseA, exception );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort() );

        // when / then
        assertThrows( exception, bootstrap::perform );
    }

    TopologyService topologyService( NamedDatabaseId namedDatabaseId, MemberId upstreamMember, SocketAddress address ) throws CatchupAddressResolutionException
    {
        TopologyService topologyService = mock( TopologyService.class );
        Map<MemberId,CoreServerInfo> members = Map.of( upstreamMember, mock( CoreServerInfo.class ) );
        when( topologyService.allCoreServers() )
                .thenReturn( members );
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) )
                .thenReturn( new DatabaseCoreTopology( namedDatabaseId.databaseId(), raftId, members ) );
        when( topologyService.lookupCatchupAddress( upstreamMember ) )
                .thenReturn( address );
        return topologyService;
    }

    private CatchupComponentsRepository.CatchupComponents catchupComponents( SocketAddress address, StoreId remoteStoreId )
            throws StoreIdDownloadFailedException
    {
        RemoteStore remoteStore = mock( RemoteStore.class );
        when( remoteStore.getStoreId( address ) ).thenReturn( remoteStoreId );
        StoreCopyProcess storeCopyProcess = mock( StoreCopyProcess.class );
        return new CatchupComponentsRepository.CatchupComponents( remoteStore, storeCopyProcess );
    }

    private DatabaseStartAborter neverAbort()
    {
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( false );
        return aborter;
    }

    private ReadReplicaDatabaseContext normalDatabase( NamedDatabaseId namedDatabaseId, StoreId storeId, Boolean isEmpty ) throws IOException
    {
        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.readStoreId( any(), any() ) ).thenReturn( storeId );
        when( storeFiles.isEmpty( any() ) ).thenReturn( isEmpty );

        Database kernelDatabase = mock( Database.class );
        when( kernelDatabase.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
        when( kernelDatabase.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );

        LogFiles txLogs = mock( LogFiles.class );
        return new ReadReplicaDatabaseContext( kernelDatabase, new Monitors(), new Dependencies(), storeFiles, txLogs, new ClusterInternalDbmsOperator(),
                NULL );
    }

    private ReadReplicaDatabaseContext failToReadLocalStoreId( NamedDatabaseId namedDatabaseId, Class<? extends Throwable> throwableClass ) throws IOException
    {
        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.readStoreId( any(), any() ) ).thenThrow( throwableClass );

        Database kernelDatabase = mock( Database.class );
        when( kernelDatabase.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
        when( kernelDatabase.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );

        LogFiles txLogs = mock( LogFiles.class );
        return new ReadReplicaDatabaseContext( kernelDatabase, new Monitors(), new Dependencies(), storeFiles, txLogs, new ClusterInternalDbmsOperator(),
                NULL );
    }

    private UpstreamDatabaseStrategySelector chooseFirstMember( TopologyService topologyService )
    {
        AlwaysChooseFirstMember firstMember = new AlwaysChooseFirstMember();
        firstMember.inject( topologyService, Config.defaults(), nullLogProvider(), null );
        return new UpstreamDatabaseStrategySelector( firstMember );
    }

    @ServiceProvider
    public static class AlwaysChooseFirstMember extends UpstreamDatabaseSelectionStrategy
    {
        AlwaysChooseFirstMember()
        {
            super( "always-choose-first-member" );
        }

        @Override
        public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
        {
            DatabaseCoreTopology coreTopology = topologyService.coreTopologyForDatabase( namedDatabaseId );
            return Optional.ofNullable( coreTopology.members().keySet().iterator().next() );
        }
    }
}
