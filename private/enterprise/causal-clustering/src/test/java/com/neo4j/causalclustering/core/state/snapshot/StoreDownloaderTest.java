/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.common.StubClusteredDatabaseContext;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;

class StoreDownloaderTest
{
    private final SocketAddress primaryAddress = new SocketAddress( "primary", 1 );
    private final SocketAddress secondaryAddress = new SocketAddress( "secondary", 2 );

    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final DatabaseId databaseId = databaseIdRepository.getByName( "target" ).get();
    private final StoreId storeId = randomStoreId();

    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();
    private final CatchupComponentsRepository components = new CatchupComponentsRepository( databaseManager );
    private final StoreDownloader downloader = new StoreDownloader( components, NullLogProvider.getInstance() );

    @Test
    void shouldReplaceMismatchedStoreIfEmpty() throws Exception
    {
        // given
        StoreDownloadContext databaseContext = mockLocalDatabase( databaseId, true, storeId );

        RemoteStore remoteStore = getRemoteStore( databaseId );
        StoreId mismatchedStoreId = randomStoreId();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( mismatchedStoreId );

        // when
        boolean downloadOk = downloader.bringUpToDate( databaseContext, primaryAddress, new SingleAddressProvider( secondaryAddress ) );

        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );

        // then
        assertTrue( downloadOk );
    }

    @Test
    void shouldNotReplaceMismatchedNonEmptyStore() throws Exception
    {
        // given
        StoreDownloadContext databaseContext = mockLocalDatabase( databaseId, false, storeId );

        RemoteStore remoteStore = getRemoteStore( databaseId );
        StoreId mismatchedStoreId = randomStoreId();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( mismatchedStoreId );

        // when
        boolean downloadOk = downloader.bringUpToDate( databaseContext, primaryAddress, new SingleAddressProvider( secondaryAddress ) );

        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );

        // then
        assertFalse( downloadOk );
    }

    @Test
    void shouldOnlyCatchupIfPossible() throws Exception
    {
        // given
        StoreDownloadContext databaseContext = mockLocalDatabase( databaseId, false, storeId );
        RemoteStore remoteStore = mockRemoteSuccessfulStore( databaseId );

        // when
        boolean downloadOk = downloader.bringUpToDate( databaseContext, primaryAddress, new SingleAddressProvider( secondaryAddress ) );

        // then
        verify( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );

        assertTrue( downloadOk );
    }

    @Test
    void shouldDownloadWholeStoreIfCannotCatchup() throws Exception
    {
        // given
        StoreDownloadContext databaseContext = mockLocalDatabase( databaseId, false, storeId );
        RemoteStore remoteStore = mockRemoteStoreCopyFailure( databaseId );
        StoreCopyProcess storeCopyProcess = getStoreCopyProcess( databaseId );

        // when
        boolean downloadOk = downloader.bringUpToDate( databaseContext, primaryAddress, new SingleAddressProvider( secondaryAddress ) );

        // then
        verify( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        verify( storeCopyProcess ).replaceWithStoreFrom( any(), any() );

        assertTrue( downloadOk );
    }

    private StoreCopyProcess getStoreCopyProcess( DatabaseId databaseId )
    {
        return components.componentsFor( databaseId ).map( CatchupComponents::storeCopyProcess ).orElseThrow();
    }

    @Test
    void shouldThrowIfComponentsDoNotExist()
    {
        // given
        StoreDownloadContext wrongDb = mock( StoreDownloadContext.class );
        when( wrongDb.databaseId() ).thenReturn( databaseIdRepository.getByName( "wrong" ).get() );

        // when & then
        assertThrows( IllegalStateException.class, () -> downloader.bringUpToDate( wrongDb, primaryAddress, new SingleAddressProvider( secondaryAddress ) ) );
    }

    private StoreDownloadContext mockLocalDatabase( DatabaseId databaseId, boolean isEmpty, StoreId storeId ) throws IOException
    {
        StubClusteredDatabaseContext db = databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId ).withCatchupComponentsFactory(
                ignored -> new CatchupComponents( mock( RemoteStore.class ), mock( StoreCopyProcess.class ) ) ).withStoreId( storeId ).register();

        db.setEmpty( isEmpty );

        Database database = mock( Database.class );
        when( database.getDatabaseId() ).thenReturn( databaseId );
        when( database.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );

        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.isEmpty( any() ) ).thenReturn( isEmpty );
        when( storeFiles.readStoreId( any() ) ).thenReturn( storeId );

        LogFiles transactionLogs = mock( LogFiles.class );

        return new StoreDownloadContext( database, storeFiles, transactionLogs, new ClusterInternalDbmsOperator() );
    }

    private RemoteStore mockRemoteSuccessfulStore( DatabaseId databaseId ) throws StoreIdDownloadFailedException
    {
        RemoteStore remoteStore = getRemoteStore( databaseId );
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( storeId );
        return remoteStore;
    }

    private RemoteStore mockRemoteStoreCopyFailure( DatabaseId databaseId ) throws StoreIdDownloadFailedException, StoreCopyFailedException, IOException
    {
        RemoteStore remoteStore = getRemoteStore( databaseId );
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( storeId );
        doThrow( StoreCopyFailedException.class ).when( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        return remoteStore;
    }

    private RemoteStore getRemoteStore( DatabaseId databaseId )
    {
        return components.componentsFor( databaseId ).map( CatchupComponents::remoteStore ).orElseThrow();
    }

    private static StoreId randomStoreId()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return new StoreId( rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong() );
    }
}
