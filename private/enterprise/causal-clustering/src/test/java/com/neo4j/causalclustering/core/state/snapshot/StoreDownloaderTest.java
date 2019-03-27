/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.identity.StoreId;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.catchup.CatchupAddressProvider.fromSingleAddress;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoreDownloaderTest
{
    private final AdvertisedSocketAddress primaryAddress = new AdvertisedSocketAddress( "primary", 1 );
    private final AdvertisedSocketAddress secondaryAddress = new AdvertisedSocketAddress( "secondary", 2 );

    private final String databaseName = "target";
    private final StoreId storeId = randomStoreId();

    private final StubCatchupComponentsRepository components = new StubCatchupComponentsRepository();
    private final StoreDownloader downloader = new StoreDownloader( components, NullLogProvider.getInstance() );

    private ClusteredDatabaseContext mockLocalDatabase( String databaseName, boolean isEmpty, StoreId storeId ) throws IOException
    {
        ClusteredDatabaseContext clusteredDatabaseContext = mock( ClusteredDatabaseContext.class );
        when( clusteredDatabaseContext.isEmpty() ).thenReturn( isEmpty );
        when( clusteredDatabaseContext.storeId() ).thenReturn( storeId );
        when( clusteredDatabaseContext.databaseName() ).thenReturn( databaseName );
        return clusteredDatabaseContext;
    }

    @Before
    public void setup() throws StoreIdDownloadFailedException, IOException, StoreCopyFailedException
    {
        components.getOrCreate( databaseName );

        // create some more components just to test that they're not mixed up
        mockRemoteUnsuccessfulStore( "other.db ", randomStoreId() );
        mockRemoteUnsuccessfulStore( "other.db ", randomStoreId() );
    }

    private StoreId randomStoreId()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return new StoreId( rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong() );
    }

    @Test
    public void shouldReplaceMismatchedStoreIfEmpty() throws Exception
    {
        // given
        ClusteredDatabaseContext database = mockLocalDatabase( databaseName, true, storeId );

        RemoteStore remoteStore = components.getOrCreate( "target" ).remoteStore();
        StoreId mismatchedStoreId = randomStoreId();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( mismatchedStoreId );

        // when
        boolean downloadOk = downloader.bringUpToDate( database, primaryAddress, fromSingleAddress( secondaryAddress ) );

        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );

        // then
        assertTrue( downloadOk );
    }

    @Test
    public void shouldNotReplaceMismatchedNonEmptyStore() throws Exception
    {
        // given
        ClusteredDatabaseContext database = mockLocalDatabase( databaseName, false, storeId );

        RemoteStore remoteStore = components.getOrCreate( databaseName ).remoteStore();
        StoreId mismatchedStoreId = randomStoreId();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( mismatchedStoreId );

        // when
        boolean downloadOk = downloader.bringUpToDate( database, primaryAddress, fromSingleAddress( secondaryAddress ) );

        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );

        // then
        assertFalse( downloadOk );
    }

    @Test
    public void shouldOnlyCatchupIfPossible() throws Exception
    {
        // given
        ClusteredDatabaseContext database = mockLocalDatabase( databaseName, false, storeId );
        RemoteStore remoteStore = mockRemoteSuccessfulStore( databaseName, storeId );

        // when
        boolean downloadOk = downloader.bringUpToDate( database, primaryAddress, fromSingleAddress( secondaryAddress ) );

        // then
        verify( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );

        assertTrue( downloadOk );
    }

    @Test
    public void shouldDownloadWholeStoreIfCannotCatchup() throws Exception
    {
        // given
        ClusteredDatabaseContext database = mockLocalDatabase( databaseName, false, storeId );
        RemoteStore remoteStore = mockRemoteUnsuccessfulStore( databaseName, storeId );
        StoreCopyProcess storeCopyProcess = components.getOrCreate( databaseName ).storeCopyProcess();

        // when
        boolean downloadOk = downloader.bringUpToDate( database, primaryAddress, fromSingleAddress( secondaryAddress ) );

        // then
        verify( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        verify( storeCopyProcess ).replaceWithStoreFrom( any(), any() );

        assertTrue( downloadOk );
    }

    @Test
    public void shouldThrowIfComponentsDoNotExist() throws Exception
    {
        // given
        ClusteredDatabaseContext database = mockLocalDatabase( "wrong", true, storeId );

        // when
        try
        {
            downloader.bringUpToDate( database, primaryAddress, fromSingleAddress( secondaryAddress ) );
            fail();
        }
        catch ( IllegalStateException ignored )
        {
            // expected
        }
    }

    private RemoteStore mockRemoteSuccessfulStore( String databaseName, StoreId storeId )
            throws StoreIdDownloadFailedException
    {
        RemoteStore remoteStore = components.getOrCreate( databaseName ).remoteStore();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( storeId );
        return remoteStore;
    }

    private RemoteStore mockRemoteUnsuccessfulStore( String databaseName, StoreId storeId )
            throws StoreIdDownloadFailedException, StoreCopyFailedException, IOException
    {
        RemoteStore remoteStore = components.getOrCreate( databaseName ).remoteStore();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( storeId );
        doThrow( StoreCopyFailedException.class ).when( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        return remoteStore;
    }

    private static class StubCatchupComponentsRepository implements CatchupComponentsRepository
    {
        private final Map<String,DatabaseCatchupComponents> componentsMap = new HashMap<>();

        private DatabaseCatchupComponents getOrCreate( String databaseName )
        {
            return componentsMap.computeIfAbsent( databaseName,
                    ignored -> new DatabaseCatchupComponents( mock( RemoteStore.class ), mock( StoreCopyProcess.class ) ) );
        }

        @Override
        public Optional<DatabaseCatchupComponents> componentsFor( String databaseName )
        {
            return Optional.ofNullable( componentsMap.get( databaseName ) );
        }
    }
}
