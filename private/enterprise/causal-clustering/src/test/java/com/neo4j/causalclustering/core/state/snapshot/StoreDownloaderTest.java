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
import com.neo4j.causalclustering.common.StubClusteredDatabaseContext;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.identity.StoreId;
import org.junit.Test;

import java.io.IOException;
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
    private final org.neo4j.storageengine.api.StoreId storeId = randomKernelStoreId();

    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();
    private final CatchupComponentsRepository components = new CatchupComponentsRepository( databaseManager );
    private final StoreDownloader downloader = new StoreDownloader( components, NullLogProvider.getInstance() );

    @Test
    public void shouldReplaceMismatchedStoreIfEmpty() throws Exception
    {
        // given
        ClusteredDatabaseContext databaseContext = mockLocalDatabase( databaseName, true, storeId );

        RemoteStore remoteStore = databaseContext.catchupComponents().remoteStore();
        StoreId mismatchedStoreId = new StoreId( randomKernelStoreId() );
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( mismatchedStoreId );

        // when
        boolean downloadOk = downloader.bringUpToDate( databaseContext, primaryAddress, fromSingleAddress( secondaryAddress ) );

        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );

        // then
        assertTrue( downloadOk );
    }

    @Test
    public void shouldNotReplaceMismatchedNonEmptyStore() throws Exception
    {
        // given
        ClusteredDatabaseContext databaseContext = mockLocalDatabase( databaseName, false, storeId );

        RemoteStore remoteStore = databaseContext.catchupComponents().remoteStore();
        StoreId mismatchedStoreId = new StoreId( randomKernelStoreId() );
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( mismatchedStoreId );

        // when
        boolean downloadOk = downloader.bringUpToDate( databaseContext, primaryAddress, fromSingleAddress( secondaryAddress ) );

        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );

        // then
        assertFalse( downloadOk );
    }

    @Test
    public void shouldOnlyCatchupIfPossible() throws Exception
    {
        // given
        ClusteredDatabaseContext databaseContext = mockLocalDatabase( databaseName, false, storeId );
        RemoteStore remoteStore = mockRemoteSuccessfulStore( databaseContext );

        // when
        boolean downloadOk = downloader.bringUpToDate( databaseContext, primaryAddress, fromSingleAddress( secondaryAddress ) );

        // then
        verify( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );

        assertTrue( downloadOk );
    }

    @Test
    public void shouldDownloadWholeStoreIfCannotCatchup() throws Exception
    {
        // given
        ClusteredDatabaseContext databaseContext = mockLocalDatabase( databaseName, false, storeId );
        RemoteStore remoteStore = mockRemoteUnsuccessfulStore( databaseContext );
        StoreCopyProcess storeCopyProcess = databaseContext.catchupComponents().storeCopyProcess();

        // when
        boolean downloadOk = downloader.bringUpToDate( databaseContext, primaryAddress, fromSingleAddress( secondaryAddress ) );

        // then
        verify( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        verify( storeCopyProcess ).replaceWithStoreFrom( any(), any() );

        assertTrue( downloadOk );
    }

    @Test
    public void shouldThrowIfComponentsDoNotExist() throws Exception
    {
        // given
        ClusteredDatabaseContext wrongDb = mock( ClusteredDatabaseContext.class );
        when( wrongDb.databaseName() ).thenReturn( "wrong" );

        // when
        try
        {
            downloader.bringUpToDate( wrongDb, primaryAddress, fromSingleAddress( secondaryAddress ) );
            fail();
        }
        catch ( IllegalStateException ignored )
        {
            // expected
        }
    }

    private ClusteredDatabaseContext mockLocalDatabase( String databaseName, boolean isEmpty, org.neo4j.storageengine.api.StoreId storeId )
    {
        StubClusteredDatabaseContext db = databaseManager.givenDatabaseWithConfig()
                .withDatabaseName( databaseName )
                .withCatchupComponentsFactory( ignored ->
                        new CatchupComponentsRepository.DatabaseCatchupComponents( mock( RemoteStore.class ), mock( StoreCopyProcess.class ) ) )
                .withKernelStoreId( storeId )
                .register();

        db.setEmpty( isEmpty );

        return db;
    }

    private RemoteStore mockRemoteSuccessfulStore( ClusteredDatabaseContext databaseContext )
            throws StoreIdDownloadFailedException
    {
        RemoteStore remoteStore = databaseContext.catchupComponents().remoteStore();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( new StoreId( storeId ) );
        return remoteStore;
    }

    private RemoteStore mockRemoteUnsuccessfulStore( ClusteredDatabaseContext databaseContext )
            throws StoreIdDownloadFailedException, StoreCopyFailedException, IOException
    {
        RemoteStore remoteStore = databaseContext.catchupComponents().remoteStore();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( new StoreId( storeId ) );
        doThrow( StoreCopyFailedException.class ).when( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        return remoteStore;
    }

    private org.neo4j.storageengine.api.StoreId randomKernelStoreId()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return new org.neo4j.storageengine.api.StoreId( rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong() );
    }

}
