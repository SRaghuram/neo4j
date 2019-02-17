/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.PerDatabaseCatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.common.LocalDatabase;
import com.neo4j.causalclustering.common.StubLocalDatabaseService;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import com.neo4j.causalclustering.helpers.FakeExecutor;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.StoreId;
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

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReadReplicaStartupProcessTest
{
    private ConstantTimeTimeoutStrategy retryStrategy = new ConstantTimeTimeoutStrategy( 1, MILLISECONDS );
    private TopologyService topologyService = mock( TopologyService.class );
    private CoreTopology clusterTopology = mock( CoreTopology.class );
    private Lifecycle txPulling = mock( Lifecycle.class );
    private CatchupComponentsRepository catchupComponents = mock( CatchupComponentsRepository.class );

    private List<String> databaseNames = asList( "db1", "db2" );
    private Map<String,LocalDatabase> registeredDbs = new HashMap<>();
    private DatabaseService databaseService = Mockito.spy( new StubLocalDatabaseService( registeredDbs ) );
    private Map<String,PerDatabaseCatchupComponents> dbCatchupComponents = new HashMap<>();
    private MemberId memberId = new MemberId( UUID.randomUUID() );
    private AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "127.0.0.1", 123 );
    private StoreId otherStoreId = new StoreId( 5, 6, 7, 8 );

    @BeforeEach
    void commonMocking() throws CatchupAddressResolutionException
    {
        Map<MemberId,CoreServerInfo> members = new HashMap<>();
        members.put( memberId, mock( CoreServerInfo.class ) );
        when( topologyService.allCoreServers() ).thenReturn( clusterTopology );
        when( clusterTopology.members() ).thenReturn( members );
        when( topologyService.findCatchupAddress( memberId ) ).thenReturn( fromAddress );
        //I know ... I'm sorry
        when( catchupComponents.componentsFor( anyString() ) )
                .then( arg -> Optional.ofNullable( dbCatchupComponents.get( arg.<String>getArgument( 0 ) ) ) );
    }

    private void mockCatchupComponents( String databaseName, LocalDatabase localDatabase, RemoteStore remoteStore,
            StoreCopyProcess storeCopyProcess )
    {
        registeredDbs.put( databaseName, localDatabase );
        dbCatchupComponents.put( databaseName, new PerDatabaseCatchupComponents( remoteStore, storeCopyProcess ) );
    }

    private void mockDatabaseResponses( String databaseName, boolean isEmpty ) throws Throwable
    {
        mockDatabaseResponses( databaseName, isEmpty, Optional.empty() );
    }

    private <E extends Exception> void failToGetFirstStoreId( String databaseName, Class<E> eClass ) throws StoreIdDownloadFailedException
    {
        RemoteStore remoteStore = mock( RemoteStore.class );
        LocalDatabase localDatabase = mockDatabase( databaseName );
        when( remoteStore.getStoreId( any() ) ).thenThrow( eClass ).thenReturn( localDatabase.storeId() );
        mockCatchupComponents( databaseName, localDatabase, remoteStore, mock( StoreCopyProcess.class ) );
    }

    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private void mockDatabaseResponses( String databaseName, boolean isEmpty, Optional<StoreId> storeIdResponse ) throws Throwable
    {
        LocalDatabase mockDb = mockDatabase( databaseName );
        when( mockDb.isEmpty() ).thenReturn( isEmpty );
        RemoteStore mockRemoteStore = mock( RemoteStore.class );
        StoreId mockStoreId = mockDb.storeId();
        when( mockRemoteStore.getStoreId( any() ) ).thenReturn( storeIdResponse.orElse( mockStoreId ) );

        mockCatchupComponents( databaseName, mockDb, mockRemoteStore, mock( StoreCopyProcess.class ) );
    }

    private LocalDatabase mockDatabase( String databaseName )
    {
        Random rng = new Random( databaseName.hashCode() );
        LocalDatabase localDatabase = mock( LocalDatabase.class );
        //Common per database mocking
        StoreId storeId = new StoreId( rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong() );
        DatabaseLayout databaseLayout = DatabaseLayout.of( new File( databaseName + "-store-dir" ) );
        when( localDatabase.storeId() ).thenReturn( storeId );
        when( localDatabase.databaseLayout() ).thenReturn( databaseLayout );
        when( localDatabase.databaseName() ).thenReturn( databaseName );
        return localDatabase;
    }

    private UpstreamDatabaseStrategySelector chooseFirstMember()
    {
        AlwaysChooseFirstMember firstMember = new AlwaysChooseFirstMember();
        Config config = mock( Config.class );
        when( config.get( CausalClusteringSettings.database ) ).thenReturn( "default" );
        firstMember.inject( topologyService, config, NullLogProvider.getInstance(), null);

        return new UpstreamDatabaseStrategySelector( firstMember );
    }

    @Test
    void shouldRetryIfDbThatThrewExpectedException() throws Throwable
    {
        // given
        Iterator<String> iterator = databaseNames.iterator();
        String db1 = iterator.next();
        failToGetFirstStoreId( db1, StoreIdDownloadFailedException.class );

        while ( iterator.hasNext() )
        {
            mockDatabaseResponses( iterator.next(), false );
        }

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), databaseService, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( databaseService ).start();
        verify( txPulling ).start();
    }

    @Test
    void shouldThrowIfNotExpectedExceptionIsThrown() throws Throwable
    {
        // given
        Iterator<String> iterator = databaseNames.iterator();
        String db1 = iterator.next();
        failToGetFirstStoreId( db1, RuntimeException.class );

        while ( iterator.hasNext() )
        {
            mockDatabaseResponses( iterator.next(), false );
        }

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), databaseService, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        Assertions.assertThrows( RuntimeException.class, readReplicaStartupProcess::start );
    }

    @Test
    void shouldReplaceEmptyStoreWithRemote() throws Throwable
    {
        // given
        for ( String name : databaseNames )
        {
            mockDatabaseResponses( name, true, Optional.of( otherStoreId ) );
        }

        when( topologyService.findCatchupAddress( any() )).thenReturn( fromAddress );
        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), databaseService, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( databaseService ).start();
        for ( PerDatabaseCatchupComponents dbCatchupComponent : dbCatchupComponents.values() )
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
        for ( String name : databaseNames )
        {
            LocalDatabase mockDb = mockDatabase( name );
            when( mockDb.isEmpty() ).thenReturn( emptyStoreToggle );

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
            StoreId mockStoreId = mockDb.storeId();
            when( mockRemoteStore.getStoreId( any() ) ).thenReturn( mockStoreId );

            mockCatchupComponents( name, mockDb, mockRemoteStore, storeCopyProcess );
        }

        when( topologyService.findCatchupAddress( any() )).thenReturn( fromAddress );
        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), databaseService, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( databaseService ).start();
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
        String name = databaseNames.get( 0 );
        mockDatabaseResponses( name, false, Optional.of( otherStoreId ) );

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), databaseService, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
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
        for ( String name : databaseNames )
        {
            mockDatabaseResponses( name, false );
        }

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), databaseService, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( databaseService ).start();
        verify( txPulling ).start();
    }

    @Test
    void stopShouldStopTheDatabaseAndStopPolling() throws Throwable
    {
        // given
        for ( String name : databaseNames )
        {
            mockDatabaseResponses( name, false );
        }

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( new FakeExecutor(), databaseService, txPulling, chooseFirstMember(), NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), topologyService, catchupComponents, retryStrategy );

        // when
        readReplicaStartupProcess.start();

        // when
        readReplicaStartupProcess.stop();

        // then
        verify( txPulling ).stop();
        verify( databaseService ).stop();
    }

    public static class AlwaysChooseFirstMember extends UpstreamDatabaseSelectionStrategy
    {
        public AlwaysChooseFirstMember()
        {
            super( "always-choose-first-member" );
        }

        @Override
        public Optional<MemberId> upstreamDatabase()
        {
            CoreTopology coreTopology = topologyService.allCoreServers();
            return Optional.ofNullable( coreTopology.members().keySet().iterator().next() );
        }
    }
}
