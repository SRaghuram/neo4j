/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.DatabaseStartAborter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.readreplica.ReadReplicaBootstrapTest.Outcome.DELETE_AND_STORE_COPY;
import static com.neo4j.causalclustering.readreplica.ReadReplicaBootstrapTest.Outcome.DO_NOTHING;
import static com.neo4j.causalclustering.readreplica.ReadReplicaBootstrapTest.Outcome.THROW_EXCEPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.exponential;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;

class ReadReplicaBootstrapTest
{
    private final NamedDatabaseId namedDatabaseA = TestDatabaseIdRepository.randomNamedDatabaseId();
    private final RaftGroupId raftGroupId = RaftGroupId.from( namedDatabaseA.databaseId() );
    private final ServerId serverA = IdFactory.randomServerId();
    private final SocketAddress addressA = new SocketAddress( "127.0.0.1", 123 );
    private final StoreId storeA = new StoreId( 0, 1, 2, 3, 4 );
    private final StoreId storeB = new StoreId( 5, 6, 7, 8, 9 );
    private final TimeoutStrategy timeoutStrategy = exponential( 100, 3000, TimeUnit.MILLISECONDS );
    private final ClusterSystemGraphDbmsModel systemDbmsModel = mock( ClusterSystemGraphDbmsModel.class );

    private ReadReplicaBootstrap createBootstrap( TopologyService topologyService, CatchupComponentsRepository.CatchupComponents catchupComponents,
            ReadReplicaDatabaseContext databaseContext, DatabaseStartAborter aborter )
    {
        return createBootstrap( topologyService, catchupComponents, databaseContext, aborter, chooseFirstMember( topologyService ), timeoutStrategy,
                                systemDbmsModel );
    }

    private ReadReplicaBootstrap createBootstrap( TopologyService topologyService, CatchupComponentsRepository.CatchupComponents catchupComponents,
                                                  ReadReplicaDatabaseContext databaseContext, ClusterSystemGraphDbmsModel systemDbmsModel )
    {
        return createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort(), chooseFirstMember( topologyService ), timeoutStrategy,
                                systemDbmsModel );
    }

    private ReadReplicaBootstrap createBootstrap( TopologyService topologyService, CatchupComponentsRepository.CatchupComponents catchupComponents,
                                                  ReadReplicaDatabaseContext databaseContext, DatabaseStartAborter aborter,
                                                  UpstreamDatabaseStrategySelector selectionStrategy,
                                                  TimeoutStrategy timeoutStrategy, ClusterSystemGraphDbmsModel systemDbmsModel )
    {
        return new ReadReplicaBootstrap( databaseContext, selectionStrategy, nullLogProvider(), nullLogProvider(), topologyService,
                                         () -> catchupComponents, new ClusterInternalDbmsOperator( nullLogProvider() ), aborter, timeoutStrategy,
                                         new CommandIndexTracker(), systemDbmsModel );
    }

    @Test
    void shouldTryAllUpstreamMembersBeforeBackingOff() throws Exception
    {
        // given
        var memberB = IdFactory.randomServerId();
        var topologyService = mock( TopologyService.class );
        var members = Map.of( serverA, mock( CoreServerInfo.class ),
                memberB, mock( CoreServerInfo.class ) );
        when( topologyService.allCoreServers() )
                .thenReturn( members );
        when( topologyService.coreTopologyForDatabase( namedDatabaseA ) )
                .thenReturn( new DatabaseCoreTopology( namedDatabaseA.databaseId(), raftGroupId, members ) );
        when( topologyService.lookupCatchupAddress( any( ServerId.class ) ) )
                .thenThrow( new CatchupAddressResolutionException( serverA ) )
                .thenThrow( new CatchupAddressResolutionException( serverA ) )
                .thenReturn( addressA );

        var catchupComponents = catchupComponents( addressA, storeA );
        var databaseContext = normalDatabase( namedDatabaseA, createStoreFiles( storeA, false, Optional.empty() ) );

        var timeoutStrategy = mock( TimeoutStrategy.class );
        var timeout = mock( TimeoutStrategy.Timeout.class );
        when( timeoutStrategy.newTimeout() ).thenReturn( timeout );
        when( timeout.getAndIncrement() ).thenReturn( 10L );
        when( timeout.getMillis() ).thenReturn( 10L );
        var bootstrapper = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort(),
                                            chooseRandomCore( topologyService ), timeoutStrategy, systemDbmsModel );

        var inOrder = inOrder( topologyService, timeout );

        // when
        bootstrapper.perform();

        // then
        inOrder.verify( topologyService, times( 2 ) ).lookupCatchupAddress( any( ServerId.class ) );
        inOrder.verify( timeout ).getMillis();
        inOrder.verify( timeout ).increment();
    }

    @Test
    void shouldFailToStartWhenStartAborted() throws Throwable
    {
        // given
        var topologyService = topologyService( namedDatabaseA, serverA, addressA );

        // Catchup components should throw an expected exception to cause a retry
        var catchupComponents = catchupComponents( addressA, storeA );
        when( catchupComponents.remoteStore().getStoreId( addressA ) )
                .thenThrow( StoreIdDownloadFailedException.class )
                .thenReturn( storeA );

        var databaseContext = normalDatabase( namedDatabaseA, createStoreFiles( storeA, false, Optional.empty() ) );

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
        var topologyService = topologyService( namedDatabaseA, serverA, addressA );
        var catchupComponents = catchupComponents( addressA, storeA );
        var databaseContext = normalDatabase( namedDatabaseA, createStoreFiles( storeA, false, Optional.empty() ) );
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
        var topologyService = topologyService( namedDatabaseA, serverA, addressA );
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
        TopologyService topologyService = topologyService( namedDatabaseA, serverA, addressA );

        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        when( catchupComponents.remoteStore().getStoreId( addressA ) )
                .thenThrow( StoreIdDownloadFailedException.class )
                .thenReturn( storeA );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, createStoreFiles( storeA, false, Optional.empty() ) );

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
        TopologyService topologyService = topologyService( namedDatabaseA, serverA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeB );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, createStoreFiles( storeA, true, Optional.empty() ) );

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
        TopologyService topologyService = topologyService( namedDatabaseA, serverA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeB );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, createStoreFiles( storeA, false, Optional.empty() ) );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort() );

        // when / then
        RuntimeException ex = assertThrows( RuntimeException.class, bootstrap::perform );
        assertThat( ex.getMessage(), allOf(
                containsString( "This read replica cannot join the cluster." ),
                containsString( "is not empty" ),
                containsString( "mismatching storeId" )) );
    }

    @Test
    void shouldStartWithMatchingDatabase() throws Throwable
    {
        // given
        TopologyService topologyService = topologyService( namedDatabaseA, serverA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, createStoreFiles( storeA, false, Optional.empty() ) );

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
        TopologyService topologyService = topologyService( namedDatabaseA, serverA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, createStoreFiles( storeA, true, Optional.empty() ) );

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
        TopologyService topologyService = topologyService( namedDatabaseA, serverA, addressA );
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponents( addressA, storeA );

        Class<RuntimeException> exception = RuntimeException.class;
        ReadReplicaDatabaseContext databaseContext = failToReadLocalStoreId( namedDatabaseA, exception );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, neverAbort() );

        // when / then
        assertThrows( exception, bootstrap::perform );
    }

    static Stream<Arguments> truthTable()
    {
        return Stream.of( Arguments.of( false, false, false, false, DELETE_AND_STORE_COPY ),
                          Arguments.of( false, false, false, true, DELETE_AND_STORE_COPY ),
                          Arguments.of( false, false, true, false, DELETE_AND_STORE_COPY ),
                          Arguments.of( false, false, true, true, DELETE_AND_STORE_COPY ),
                          Arguments.of( false, true, false, false, THROW_EXCEPTION ),
                          Arguments.of( false, true, false, true, THROW_EXCEPTION ),
                          Arguments.of( false, true, true, false, DO_NOTHING ),
                          Arguments.of( false, true, true, true, DO_NOTHING ),
                          Arguments.of( true, false, false, false, DELETE_AND_STORE_COPY ),
                          Arguments.of( true, false, false, true, DELETE_AND_STORE_COPY ),
                          Arguments.of( true, false, true, false, DELETE_AND_STORE_COPY ),
                          Arguments.of( true, false, true, true, DELETE_AND_STORE_COPY ),
                          Arguments.of( true, true, false, false, DELETE_AND_STORE_COPY ),
                          Arguments.of( true, true, false, true, DELETE_AND_STORE_COPY ),
                          Arguments.of( true, true, true, false, DELETE_AND_STORE_COPY ),
                          Arguments.of( true, true, true, true, DO_NOTHING ) );
    }

    @ParameterizedTest( name = "designatedSeederExist={0}, storeFilesExist={1}, storeIdMatch={2}, databaseIdMatch={3}" )
    @MethodSource( "truthTable" )
    void allPossibleCreateDatabaseBootstrapOutcomes( boolean designatedSeederExist, boolean storeFilesExist, boolean storeIdMatch, boolean databaseIdMatch,
                                                     Outcome allowedOutcome ) throws Exception
    {
        // given
        var topologyService = topologyService( namedDatabaseA, serverA, addressA );
        var catchupComponents = catchupComponents( addressA, storeA );

        var systemDbmsModel = mock( ClusterSystemGraphDbmsModel.class );
        Optional<ServerId> designatedSeederAnswer = designatedSeederExist ? Optional.of( new ServerId( UUID.randomUUID() ) ) : Optional.empty();
        when( systemDbmsModel.designatedSeeder( namedDatabaseA ) ).thenReturn( designatedSeederAnswer );

        var storeId = storeIdMatch ? storeA : storeB;
        var databaseId = databaseIdMatch ? namedDatabaseA.databaseId() : DatabaseIdFactory.from( UUID.randomUUID() );
        final var storeFiles = createStoreFiles( storeId, !storeFilesExist, Optional.of( databaseId ) );
        ReadReplicaDatabaseContext databaseContext = normalDatabase( namedDatabaseA, storeFiles );

        ReadReplicaBootstrap bootstrap = createBootstrap( topologyService, catchupComponents, databaseContext, systemDbmsModel );

        // when
        var didThrow = tryBootstrap( bootstrap );

        // then
        allowedOutcome.assertions( storeFiles, catchupComponents, storeA, namedDatabaseA.databaseId(), didThrow, databaseIdMatch );
    }

    private boolean tryBootstrap( ReadReplicaBootstrap bootstrap )
    {
        try
        {
            bootstrap.perform();
        }
        catch ( Exception e )
        {
            return true;
        }
        return false;
    }

    enum Outcome
    {
        DELETE_AND_STORE_COPY
                {
                    @Override
                    void assertions( StoreFiles storeFiles, CatchupComponentsRepository.CatchupComponents catchupComponents, StoreId upstreamStoreId,
                                     DatabaseId upstreamDatabaseId, boolean didThrow, boolean databaseIdAlreadyMath ) throws Exception
                    {
                        verify( storeFiles, atLeastOnce() ).delete( any(), any() );
                        verify( catchupComponents.storeCopyProcess(), atLeastOnce() ).replaceWithStoreFrom( any(), eq( upstreamStoreId ) );
                        assertFalse( didThrow );
                    }
                },
        THROW_EXCEPTION
                {
                    @Override
                    void assertions( StoreFiles storeFiles, CatchupComponentsRepository.CatchupComponents catchupComponents, StoreId upstreamStoreId,
                                     DatabaseId upstreamDatabaseId, boolean didThrow, boolean databaseIdAlreadyMath ) throws Exception
                    {
                        verify( storeFiles, never() ).delete( any(), any() );
                        verify( catchupComponents.storeCopyProcess(), never() ).replaceWithStoreFrom( any(), eq( upstreamStoreId ) );
                        assertTrue( didThrow );
                    }
                },
        DO_NOTHING
                {
                    @Override
                    void assertions( StoreFiles storeFiles, CatchupComponentsRepository.CatchupComponents catchupComponents, StoreId upstreamStoreId,
                                     DatabaseId upstreamDatabaseId, boolean didThrow, boolean databaseIdAlreadyMath ) throws Exception
                    {
                        verify( storeFiles, never() ).delete( any(), any() );
                        verify( catchupComponents.storeCopyProcess(), never() ).replaceWithStoreFrom( any(), eq( upstreamStoreId ) );
                        assertFalse( didThrow );
                    }
                };

        abstract void assertions( StoreFiles storeFiles, CatchupComponentsRepository.CatchupComponents catchupComponents, StoreId upstreamStoreId,
                                  DatabaseId upstreamDatabaseId, boolean didThrow, boolean databaseIdAlreadyMath ) throws Exception;
    }

    TopologyService topologyService( NamedDatabaseId namedDatabaseId, ServerId upstreamMember, SocketAddress address ) throws CatchupAddressResolutionException
    {
        TopologyService topologyService = mock( TopologyService.class );
        Map<ServerId,CoreServerInfo> members = Map.of( upstreamMember, mock( CoreServerInfo.class ) );
        when( topologyService.allCoreServers() )
                .thenReturn( members );
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) )
                .thenReturn( new DatabaseCoreTopology( namedDatabaseId.databaseId(), raftGroupId, members ) );
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
        CatchupClientFactory catchupClientFactory = mock( CatchupClientFactory.class );
        return new CatchupComponentsRepository.CatchupComponents( remoteStore, storeCopyProcess, catchupClientFactory );
    }

    private DatabaseStartAborter neverAbort()
    {
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( false );
        return aborter;
    }

    private ReadReplicaDatabaseContext normalDatabase( NamedDatabaseId namedDatabaseId, StoreFiles storeFiles ) throws IOException
    {
        Database kernelDatabase = mock( Database.class );
        final var databaseLayout = mock( DatabaseLayout.class );
        when( kernelDatabase.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
        when( kernelDatabase.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );
        when( kernelDatabase.getDatabaseLayout() ).thenReturn( databaseLayout );
        when( databaseLayout.metadataStore() ).thenReturn( Path.of( "" ) );

        LogFiles txLogs = mock( LogFiles.class );
        return new ReadReplicaDatabaseContext( kernelDatabase, new Monitors(), new Dependencies(), storeFiles, txLogs,
                                               new ClusterInternalDbmsOperator( nullLogProvider() ), NULL );
    }

    private StoreFiles createStoreFiles( StoreId storeId, Boolean isEmpty, Optional<DatabaseId> databaseId ) throws IOException
    {
        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.readStoreId( any(), any() ) ).thenReturn( storeId );
        when( storeFiles.isEmpty( any() ) ).thenReturn( isEmpty );
        when( storeFiles.readDatabaseIdFromDisk( any(), any() ) ).thenReturn( databaseId );
        return storeFiles;
    }

    private ReadReplicaDatabaseContext failToReadLocalStoreId( NamedDatabaseId namedDatabaseId, Class<? extends Throwable> throwableClass ) throws IOException
    {
        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.readStoreId( any(), any() ) ).thenThrow( throwableClass );

        Database kernelDatabase = mock( Database.class );
        when( kernelDatabase.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
        when( kernelDatabase.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );

        LogFiles txLogs = mock( LogFiles.class );
        return new ReadReplicaDatabaseContext( kernelDatabase, new Monitors(), new Dependencies(), storeFiles, txLogs,
                                               new ClusterInternalDbmsOperator( nullLogProvider() ), NULL );
    }

    private UpstreamDatabaseStrategySelector chooseFirstMember( TopologyService topologyService )
    {
        AlwaysChooseFirstMember firstMember = new AlwaysChooseFirstMember();
        firstMember.inject( topologyService, Config.defaults(), nullLogProvider(), null );
        return new UpstreamDatabaseStrategySelector( firstMember );
    }

    private UpstreamDatabaseStrategySelector chooseRandomCore( TopologyService topologyService )
    {
        var randomCoreStrategy = new ConnectToRandomCoreServerStrategy();
        randomCoreStrategy.inject( topologyService, Config.defaults(), nullLogProvider(), null );
        return new UpstreamDatabaseStrategySelector( randomCoreStrategy );
    }

    @ServiceProvider
    public static class AlwaysChooseFirstMember extends UpstreamDatabaseSelectionStrategy
    {
        AlwaysChooseFirstMember()
        {
            super( "always-choose-first-member" );
        }

        @Override
        public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId )
        {
            DatabaseCoreTopology coreTopology = topologyService.coreTopologyForDatabase( namedDatabaseId );
            return Optional.ofNullable( coreTopology.servers().keySet().iterator().next() );
        }
    }
}
