/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.DatabaseCatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.StubClusteredDatabaseContext;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import com.neo4j.causalclustering.helpers.FakeExecutor;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReadReplicaStartupProcessTest
{
    private final ConstantTimeTimeoutStrategy retryStrategy = new ConstantTimeTimeoutStrategy( 1, MILLISECONDS );
    private final TopologyService topologyService = mock( TopologyService.class );
    private final Lifecycle txPulling = mock( Lifecycle.class );
    private final CatchupComponentsRepository catchupComponents = mock( CatchupComponentsRepository.class );

    private final ClusterId clusterId = new ClusterId( UUID.randomUUID() );
    private final List<DatabaseId> databaseIds = asList( new DatabaseId( "db1" ), new DatabaseId( "db2" ) );
    private final StubClusteredDatabaseManager clusteredDatabaseManager = Mockito.spy( new StubClusteredDatabaseManager() );
    private final Map<DatabaseId,DatabaseCatchupComponents> dbCatchupComponents = new HashMap<>();
    private final MemberId memberId = new MemberId( UUID.randomUUID() );
    private final AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "127.0.0.1", 123 );
    private final StoreId otherStoreId = new StoreId( 5, 6, 7, 8, 9 );

    @BeforeEach
    void commonMocking() throws CatchupAddressResolutionException
    {
        Map<MemberId,CoreServerInfo> members = Map.of( memberId, mock( CoreServerInfo.class ) );
        when( topologyService.allCoreServers() ).thenReturn( members );
        for ( DatabaseId databaseId : databaseIds )
        {
            when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( new DatabaseCoreTopology( databaseId, clusterId, members ) );
        }
        when( topologyService.findCatchupAddress( memberId ) ).thenReturn( fromAddress );
        //I know ... I'm sorry
        when( catchupComponents.componentsFor( any( DatabaseId.class ) ) )
                .then( arg -> Optional.ofNullable( dbCatchupComponents.get( arg.<DatabaseId>getArgument( 0 ) ) ) );
    }

    private void mockCatchupComponents( DatabaseId databaseId, RemoteStore remoteStore,
            StoreCopyProcess storeCopyProcess )
    {
        dbCatchupComponents.put( databaseId, new DatabaseCatchupComponents( remoteStore, storeCopyProcess ) );
    }

    private void mockDatabaseResponses( DatabaseId databaseId ) throws Throwable
    {
        mockDatabaseResponses( databaseId, false, Optional.empty() );
    }

    private <E extends Exception> void failToGetFirstStoreId( DatabaseId databaseId, Class<E> eClass ) throws StoreIdDownloadFailedException
    {
        RemoteStore remoteStore = mock( RemoteStore.class );
        ClusteredDatabaseContext clusteredDatabaseContext = stubDatabase( databaseId );
        when( remoteStore.getStoreId( any() ) ).thenThrow( eClass ).thenReturn( clusteredDatabaseContext.storeId() );
        mockCatchupComponents( databaseId, remoteStore, mock( StoreCopyProcess.class ) );
    }

    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private void mockDatabaseResponses( DatabaseId databaseId, boolean isEmpty, Optional<StoreId> storeIdResponse ) throws Throwable
    {
        StubClusteredDatabaseContext stubDb = stubDatabase( databaseId );
        stubDb.setEmpty( isEmpty );
        RemoteStore mockRemoteStore = mock( RemoteStore.class );
        StoreId mockStoreId = stubDb.storeId();
        when( mockRemoteStore.getStoreId( any() ) ).thenReturn( storeIdResponse.orElse( mockStoreId ) );

        mockCatchupComponents( databaseId, mockRemoteStore, mock( StoreCopyProcess.class ) );
    }

    private StubClusteredDatabaseContext stubDatabase( DatabaseId databaseId )
    {
        Random rng = new Random( databaseId.hashCode() );

        StoreId storeId = new StoreId( rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong() );
        DatabaseLayout databaseLayout = DatabaseLayout.of( new File( databaseId.name() + "-store-dir" ) );
        return clusteredDatabaseManager.givenDatabaseWithConfig()
                .withDatabaseId( databaseId )
                .withStoreId( storeId )
                .withDatabaseLayout( databaseLayout )
                .register();
    }

    private UpstreamDatabaseStrategySelector chooseFirstMember()
    {
        AlwaysChooseFirstMember firstMember = new AlwaysChooseFirstMember();
        firstMember.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), null );
        return new UpstreamDatabaseStrategySelector( firstMember );
    }

    @Test
    void shouldRetryIfDbThatThrewExpectedException() throws Throwable
    {
        // given
        Iterator<DatabaseId> iterator = databaseIds.iterator();
        DatabaseId db1 = iterator.next();
        failToGetFirstStoreId( db1, StoreIdDownloadFailedException.class );

        while ( iterator.hasNext() )
        {
            mockDatabaseResponses( iterator.next() );
        }

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), clusteredDatabaseManager, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( clusteredDatabaseManager ).start();
        verify( txPulling ).start();
    }

    @Test
    void shouldThrowIfNotExpectedExceptionIsThrown() throws Throwable
    {
        // given
        Iterator<DatabaseId> iterator = databaseIds.iterator();
        DatabaseId db1 = iterator.next();
        failToGetFirstStoreId( db1, RuntimeException.class );

        while ( iterator.hasNext() )
        {
            mockDatabaseResponses( iterator.next() );
        }

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), clusteredDatabaseManager, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        Assertions.assertThrows( RuntimeException.class, readReplicaStartupProcess::start );
    }

    @Test
    void shouldReplaceEmptyStoreWithRemote() throws Throwable
    {
        // given
        for ( DatabaseId name : databaseIds )
        {
            mockDatabaseResponses( name, true, Optional.of( otherStoreId ) );
        }

        when( topologyService.findCatchupAddress( any() )).thenReturn( fromAddress );
        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), clusteredDatabaseManager, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( clusteredDatabaseManager ).start();
        for ( DatabaseCatchupComponents dbCatchupComponent : dbCatchupComponents.values() )
        {
            StoreCopyProcess storeCopy = dbCatchupComponent.storeCopyProcess();
            verify( storeCopy ).replaceWithStoreFrom( any(), any() );
        }
        verify( txPulling ).start();
    }

    @Test
    void shouldReplaceOnlyEmptyStoresWithRemote() throws Throwable
    {
        // given
        List<StoreCopyProcess> emptyStoreCopies = new ArrayList<>();
        List<StoreCopyProcess> nonEmptyStoreCopies = new ArrayList<>();
        boolean emptyStoreToggle = false;
        for ( DatabaseId name : databaseIds )
        {
            StubClusteredDatabaseContext stubDb = stubDatabase( name );
            stubDb.setEmpty( emptyStoreToggle );

            StoreCopyProcess storeCopyProcess = mock( StoreCopyProcess.class );

            if ( emptyStoreToggle )
            {
                emptyStoreCopies.add( storeCopyProcess );
            }
            else
            {
                nonEmptyStoreCopies.add( storeCopyProcess );
            }
            emptyStoreToggle = !emptyStoreToggle;
            RemoteStore mockRemoteStore = mock( RemoteStore.class );
            StoreId mockStoreId = stubDb.storeId();
            when( mockRemoteStore.getStoreId( any() ) ).thenReturn( mockStoreId );

            mockCatchupComponents( name, mockRemoteStore, storeCopyProcess );
        }

        when( topologyService.findCatchupAddress( any() )).thenReturn( fromAddress );
        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), clusteredDatabaseManager, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( clusteredDatabaseManager ).start();
        for ( StoreCopyProcess storeCopy : emptyStoreCopies )
        {
            verify( storeCopy ).replaceWithStoreFrom( any(), any() );
        }

        for ( StoreCopyProcess storeCopy : nonEmptyStoreCopies )
        {
            verify( storeCopy, never()).replaceWithStoreFrom( any(), any() );
        }
        verify( txPulling ).start();
    }

    @Test
    void shouldNotStartWithMismatchedNonEmptyStore() throws Throwable
    {
        // given
        DatabaseId name = databaseIds.get( 0 );
        mockDatabaseResponses( name, false, Optional.of( otherStoreId ) );

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), clusteredDatabaseManager, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        RuntimeException ex = Assertions.assertThrows( RuntimeException.class, readReplicaStartupProcess::start );
        //expected.
        assertThat( ex.getMessage(),
                allOf( containsString( "This read replica cannot join the cluster." ), containsString( "is not empty and has a mismatching storeId" ) ) );

        // then
        verify( txPulling, never() ).start();
    }

    @Test
    void shouldStartWithMatchingDatabase() throws Throwable
    {
        // given
        for ( DatabaseId name : databaseIds )
        {
            mockDatabaseResponses( name );
        }

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), clusteredDatabaseManager, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( clusteredDatabaseManager ).start();
        verify( txPulling ).start();
    }

    @Test
    void stopShouldStopTheDatabaseAndStopPolling() throws Throwable
    {
        // given
        for ( DatabaseId name : databaseIds )
        {
            mockDatabaseResponses( name );
        }

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), clusteredDatabaseManager, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // when
        readReplicaStartupProcess.stop();

        // then
        verify( txPulling ).stop();
        verify( clusteredDatabaseManager ).stop();
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
