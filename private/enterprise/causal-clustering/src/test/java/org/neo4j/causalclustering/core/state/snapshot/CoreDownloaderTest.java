/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.common.StubLocalDatabaseService;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupAddressProvider.fromSingleAddress;

public class CoreDownloaderTest
{
    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final AdvertisedSocketAddress remoteAddress = new AdvertisedSocketAddress( "remoteAddress", 1234 );
    private final CatchupAddressProvider addressProvider = fromSingleAddress( remoteAddress );

    private final SnapshotDownloader snapshotDownloader = mock( SnapshotDownloader.class );
    private final StoreDownloader storeDownloader = mock( StoreDownloader.class );

    private StubLocalDatabaseService databaseService = new StubLocalDatabaseService();
    private final CoreDownloader downloader = new CoreDownloader( databaseService, snapshotDownloader, storeDownloader, logProvider );
    private LocalDatabase databaseA = mock( LocalDatabase.class );
    private LocalDatabase databaseB = mock( LocalDatabase.class );

    @Before
    public void before()
    {
        databaseService.registerDatabase( "first.db", databaseA );
        databaseService.registerDatabase( "second.db", databaseB );
    }

    @Test
    public void shouldDownloadSnapshotAndStores() throws Throwable
    {
        // given
        CoreSnapshot expectedSnapshot = mock( CoreSnapshot.class );

        when( snapshotDownloader.getCoreSnapshot( remoteAddress ) ).thenReturn( Optional.of( expectedSnapshot ) );
        when( storeDownloader.bringUpToDate( databaseA, remoteAddress, addressProvider ) ).thenReturn( true );
        when( storeDownloader.bringUpToDate( databaseB, remoteAddress, addressProvider ) ).thenReturn( true );

        // when
        Optional<CoreSnapshot> snapshot = downloader.downloadSnapshotAndStores( addressProvider );

        verify( snapshotDownloader ).getCoreSnapshot( remoteAddress );
        verify( storeDownloader ).bringUpToDate( databaseA, remoteAddress, addressProvider );
        verify( storeDownloader ).bringUpToDate( databaseB, remoteAddress, addressProvider );

        // then
        assertTrue( snapshot.isPresent() );
        assertEquals( expectedSnapshot, snapshot.get() );
    }

    @Test
    public void shouldReturnEmptyWhenSnapshotDownloadFails() throws Throwable
    {
        // given
        when( snapshotDownloader.getCoreSnapshot( remoteAddress ) ).thenReturn( Optional.empty() );

        // when
        Optional<CoreSnapshot> snapshot = downloader.downloadSnapshotAndStores( addressProvider );

        verify( snapshotDownloader ).getCoreSnapshot( remoteAddress );
        verify( storeDownloader, never() ).bringUpToDate( any(), any(), any() );

        // then
        assertFalse( snapshot.isPresent() );
    }

    @Test
    public void shouldReturnEmptyWhenAnyStoreDownloadFails() throws Throwable
    {
        // given
        CoreSnapshot expectedSnapshot = mock( CoreSnapshot.class );

        when( snapshotDownloader.getCoreSnapshot( remoteAddress ) ).thenReturn( Optional.of( expectedSnapshot ) );
        when( storeDownloader.bringUpToDate( databaseA, remoteAddress, addressProvider ) ).thenReturn( true );
        when( storeDownloader.bringUpToDate( databaseB, remoteAddress, addressProvider ) ).thenReturn( false );

        // when
        Optional<CoreSnapshot> snapshot = downloader.downloadSnapshotAndStores( addressProvider );

        verify( snapshotDownloader ).getCoreSnapshot( remoteAddress );
        verify( storeDownloader ).bringUpToDate( databaseB, remoteAddress, addressProvider );

        // then
        assertFalse( snapshot.isPresent() );
    }
}
