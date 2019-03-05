/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.common.LocalDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.catchup.CatchupAddressProvider.fromSingleAddress;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoreDownloaderTest
{
    private static final String DATABASE_NAME = "the_database";

    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final AdvertisedSocketAddress remoteAddress = new AdvertisedSocketAddress( "remoteAddress", 1234 );
    private final CatchupAddressProvider addressProvider = fromSingleAddress( remoteAddress );

    private final SnapshotDownloader snapshotDownloader = mock( SnapshotDownloader.class );
    private final StoreDownloader storeDownloader = mock( StoreDownloader.class );

    private final CoreDownloader downloader = new CoreDownloader( snapshotDownloader, storeDownloader, logProvider );
    private final LocalDatabase database = mock( LocalDatabase.class );

    @BeforeEach
    void setUp()
    {
        when( database.databaseName() ).thenReturn( DATABASE_NAME );
    }

    @Test
    void shouldDownloadSnapshotAndStore() throws Throwable
    {
        // given
        CoreSnapshot expectedSnapshot = mock( CoreSnapshot.class );

        when( snapshotDownloader.getCoreSnapshot( DATABASE_NAME, remoteAddress ) ).thenReturn( Optional.of( expectedSnapshot ) );
        when( storeDownloader.bringUpToDate( database, remoteAddress, addressProvider ) ).thenReturn( true );

        // when
        Optional<CoreSnapshot> snapshot = downloader.downloadSnapshotAndStore( database, addressProvider );

        verify( snapshotDownloader ).getCoreSnapshot( DATABASE_NAME, remoteAddress );
        verify( storeDownloader ).bringUpToDate( database, remoteAddress, addressProvider );

        // then
        assertTrue( snapshot.isPresent() );
        assertEquals( expectedSnapshot, snapshot.get() );
    }

    @Test
    void shouldReturnEmptyWhenSnapshotDownloadFails() throws Throwable
    {
        // given
        when( snapshotDownloader.getCoreSnapshot( DATABASE_NAME, remoteAddress ) ).thenReturn( Optional.empty() );

        // when
        Optional<CoreSnapshot> snapshot = downloader.downloadSnapshotAndStore( database, addressProvider );

        verify( snapshotDownloader ).getCoreSnapshot( DATABASE_NAME, remoteAddress );
        verify( storeDownloader, never() ).bringUpToDate( any(), any(), any() );

        // then
        assertFalse( snapshot.isPresent() );
    }

    @Test
    void shouldReturnEmptyWhenAnyStoreDownloadFails() throws Throwable
    {
        // given
        CoreSnapshot expectedSnapshot = mock( CoreSnapshot.class );

        when( snapshotDownloader.getCoreSnapshot( DATABASE_NAME, remoteAddress ) ).thenReturn( Optional.of( expectedSnapshot ) );
        when( storeDownloader.bringUpToDate( database, remoteAddress, addressProvider ) ).thenReturn( false );

        // when
        Optional<CoreSnapshot> snapshot = downloader.downloadSnapshotAndStore( database, addressProvider );

        verify( snapshotDownloader ).getCoreSnapshot( DATABASE_NAME, remoteAddress );

        // then
        assertFalse( snapshot.isPresent() );
    }
}
