/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BackupDelegatorTest
{
    private RemoteStore remoteStore;
    private CatchupClientFactory catchUpClient;
    private StoreCopyClient storeCopyClient;

    private BackupDelegator subject;

    private final AdvertisedSocketAddress anyAddress = new AdvertisedSocketAddress( "any.address", 1234 );

    @BeforeEach
    void setup()
    {
        remoteStore = mock( RemoteStore.class );
        catchUpClient = mock( CatchupClientFactory.class );
        storeCopyClient = mock( StoreCopyClient.class );
        subject = new BackupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    @Test
    void tryCatchingUpDelegatesToRemoteStore() throws StoreCopyFailedException, IOException
    {
        // given
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "neo4j.com", 5432 );
        StoreId expectedStoreId = new StoreId( 7, 2, 3, 5, 98 );
        DatabaseLayout databaseLayout = DatabaseLayout.of( new File( "." ) );

        // when
        subject.tryCatchingUp( fromAddress, expectedStoreId, databaseLayout );

        // then
        verify( remoteStore ).tryCatchingUp( any( CatchupAddressProvider.SingleAddressProvider.class ), eq( expectedStoreId ), eq( databaseLayout ), eq( true ),
                eq( true ) );
    }

    @Test
    void startDelegatesToCatchUpClient()
    {
        // when
        subject.start();

        // then
        verify( catchUpClient ).start();
    }

    @Test
    void stopDelegatesToCatchUpClient()
    {
        // when
        subject.stop();

        // then
        verify( catchUpClient ).stop();
    }

    @Test
    void fetchStoreIdDelegatesToStoreCopyClient() throws StoreIdDownloadFailedException
    {
        // given
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "neo4.com", 935 );

        // and
        StoreId expectedStoreId = new StoreId( 6, 2, 7, 9, 3 );
        when( storeCopyClient.fetchStoreId( fromAddress ) ).thenReturn( expectedStoreId );

        // when
        StoreId storeId = subject.fetchStoreId( fromAddress );

        // then
        assertEquals( expectedStoreId, storeId );
    }

    @Test
    void retrieveStoreDelegatesToStoreCopyService()
            throws StoreCopyFailedException, CatchupAddressResolutionException
    {
        // given
        StoreId storeId = new StoreId( 92, 5, 12, 7, 32 );
        DatabaseLayout databaseLayout = DatabaseLayout.of( new File( "." ) );

        // when
        subject.copy( anyAddress, storeId, databaseLayout );

        // then
        ArgumentCaptor<CatchupAddressProvider> argumentCaptor = ArgumentCaptor.forClass( CatchupAddressProvider.class );
        verify( remoteStore ).copy( argumentCaptor.capture(), eq( storeId ), eq( databaseLayout ), eq( true ) );

        //and
        assertEquals( anyAddress, argumentCaptor.getValue().primary( new TestDatabaseIdRepository().defaultDatabase() ) );
    }
}
