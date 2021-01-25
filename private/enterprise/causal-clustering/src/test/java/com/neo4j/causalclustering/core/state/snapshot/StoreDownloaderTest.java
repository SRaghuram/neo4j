/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId namedDatabaseId = databaseIdRepository.getRaw( "target" );
    private final StoreId storeId = randomStoreId();

    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();
    private final CatchupComponentsRepository components = new CatchupComponentsRepository( databaseManager );
    private final StoreDownloader downloader = new StoreDownloader( components, NullLogProvider.getInstance() );

    @Test
    void shouldReplaceMismatchedStoreIfEmpty() throws Exception
    {
        // given
        StoreDownloadContext databaseContext = mockLocalDatabase( namedDatabaseId, true, storeId );

        RemoteStore remoteStore = getRemoteStore( namedDatabaseId );
        StoreId mismatchedStoreId = randomStoreId();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( mismatchedStoreId );

        // when
        downloader.bringUpToDate( databaseContext, primaryAddress, new SingleAddressProvider( secondaryAddress ) );

        // then
        verify( remoteStore, never() ).copy( any(), any(), any() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean() );
    }

    @Test
    void shouldNotReplaceMismatchedNonEmptyStore() throws Exception
    {
        // given
        StoreDownloadContext databaseContext = mockLocalDatabase( namedDatabaseId, false, storeId );

        RemoteStore remoteStore = getRemoteStore( namedDatabaseId );
        StoreId mismatchedStoreId = randomStoreId();
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( mismatchedStoreId );

        // when
        var exception = assertThrows( SnapshotFailedException.class,
                () -> downloader.bringUpToDate( databaseContext, primaryAddress, new SingleAddressProvider( secondaryAddress ) ) );

        verify( remoteStore, never() ).copy( any(), any(), any() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean() );

        // then
        assertEquals( exception.status(), SnapshotFailedException.Status.UNRECOVERABLE );
    }

    @Test
    void shouldOnlyCatchupIfPossible() throws Exception
    {
        // given
        StoreDownloadContext databaseContext = mockLocalDatabase( namedDatabaseId, false, storeId );
        RemoteStore remoteStore = mockRemoteSuccessfulStore( namedDatabaseId );

        // when
        downloader.bringUpToDate( databaseContext, primaryAddress, new SingleAddressProvider( secondaryAddress ) );

        // then
        verify( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean() );
        verify( remoteStore, never() ).copy( any(), any(), any() );
    }

    @Test
    void shouldDownloadWholeStoreIfCannotCatchup() throws Exception
    {
        // given
        StoreDownloadContext databaseContext = mockLocalDatabase( namedDatabaseId, false, storeId );
        RemoteStore remoteStore = mockRemoteStoreCopyFailure( namedDatabaseId );
        StoreCopyProcess storeCopyProcess = getStoreCopyProcess( namedDatabaseId );

        // when
        downloader.bringUpToDate( databaseContext, primaryAddress, new SingleAddressProvider( secondaryAddress ) );

        // then
        verify( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean() );
        verify( storeCopyProcess ).replaceWithStoreFrom( any(), any() );
    }

    private StoreCopyProcess getStoreCopyProcess( NamedDatabaseId namedDatabaseId )
    {
        return components.componentsFor( namedDatabaseId ).map( CatchupComponents::storeCopyProcess ).orElseThrow();
    }

    @Test
    void shouldThrowIfComponentsDoNotExist()
    {
        // given
        StoreDownloadContext wrongDb = mock( StoreDownloadContext.class );
        when( wrongDb.databaseId() ).thenReturn( databaseIdRepository.getRaw( "wrong" ) );

        // when & then
        assertThrows( IllegalStateException.class, () -> downloader.bringUpToDate( wrongDb, primaryAddress, new SingleAddressProvider( secondaryAddress ) ) );
    }

    private StoreDownloadContext mockLocalDatabase( NamedDatabaseId namedDatabaseId, boolean isEmpty, StoreId storeId ) throws IOException
    {
        StubClusteredDatabaseContext db = databaseManager.givenDatabaseWithConfig().withDatabaseId( namedDatabaseId ).withCatchupComponentsFactory(
                ignored -> new CatchupComponents( mock( RemoteStore.class ), mock( StoreCopyProcess.class ) ) ).withStoreId( storeId ).register();

        db.setEmpty( isEmpty );

        Database database = mock( Database.class );
        when( database.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
        when( database.getInternalLogProvider() ).thenReturn( nullDatabaseLogProvider() );

        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.isEmpty( any() ) ).thenReturn( isEmpty );
        when( storeFiles.readStoreId( any(), any() ) ).thenReturn( storeId );

        LogFiles transactionLogs = mock( LogFiles.class );

        return new StoreDownloadContext( database, storeFiles, transactionLogs,
                                         new ClusterInternalDbmsOperator( NullLogProvider.getInstance() ), PageCacheTracer.NULL );
    }

    private RemoteStore mockRemoteSuccessfulStore( NamedDatabaseId namedDatabaseId ) throws StoreIdDownloadFailedException
    {
        RemoteStore remoteStore = getRemoteStore( namedDatabaseId );
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( storeId );
        return remoteStore;
    }

    private RemoteStore mockRemoteStoreCopyFailure( NamedDatabaseId namedDatabaseId )
            throws StoreIdDownloadFailedException, StoreCopyFailedException, IOException
    {
        RemoteStore remoteStore = getRemoteStore( namedDatabaseId );
        when( remoteStore.getStoreId( primaryAddress ) ).thenReturn( storeId );
        doThrow( StoreCopyFailedException.class ).when( remoteStore ).tryCatchingUp( any(), any(), any(), anyBoolean() );
        return remoteStore;
    }

    private RemoteStore getRemoteStore( NamedDatabaseId namedDatabaseId )
    {
        return components.componentsFor( namedDatabaseId ).map( CatchupComponents::remoteStore ).orElseThrow();
    }

    private static StoreId randomStoreId()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return new StoreId( rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong() );
    }
}
