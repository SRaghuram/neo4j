/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.MockCatchupClient;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientResponses;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV3;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_3_0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BackupDelegatorTest
{
    private RemoteStore remoteStore;
    private CatchupClientFactory catchUpClientFactory;
    private StoreCopyClient storeCopyClient;

    private BackupDelegator subject;

    private final SocketAddress anyAddress = new SocketAddress( "any.address", 1234 );
    private final NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();

    @BeforeEach
    void setup()
    {
        remoteStore = mock( RemoteStore.class );
        catchUpClientFactory = mock( CatchupClientFactory.class );
        storeCopyClient = mock( StoreCopyClient.class );
        var catchUpClient = new MockCatchupClient( CATCHUP_3_0, new MockClientV3( new MockClientResponses(), new TestDatabaseIdRepository() ) );
        when( catchUpClientFactory.getClient( any( SocketAddress.class ), any( Log.class ) ) ).thenReturn( catchUpClient );
        subject = new BackupDelegator( id -> remoteStore, id -> storeCopyClient, catchUpClientFactory, NullLogProvider.getInstance() );
    }

    @Test
    void tryCatchingUpDelegatesToRemoteStore() throws StoreCopyFailedException, IOException
    {
        // given
        SocketAddress fromAddress = new SocketAddress( "neo4j.com", 5432 );
        StoreId expectedStoreId = new StoreId( 7, 2, 3, 5, 98 );
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( Path.of( "." ) );

        // when
        subject.tryCatchingUp( fromAddress, expectedStoreId, namedDatabaseId, databaseLayout );

        // then
        verify( remoteStore ).tryCatchingUp( any( CatchupAddressProvider.SingleAddressProvider.class ), eq( expectedStoreId ), eq( databaseLayout ),
                eq( true ) );
    }

    @Test
    void startDelegatesToCatchUpClient()
    {
        // when
        subject.start();

        // then
        verify( catchUpClientFactory ).start();
    }

    @Test
    void stopDelegatesToCatchUpClient()
    {
        // when
        subject.stop();

        // then
        verify( catchUpClientFactory ).stop();
    }

    @Test
    void fetchStoreIdDelegatesToStoreCopyClient() throws StoreIdDownloadFailedException
    {
        // given
        SocketAddress fromAddress = new SocketAddress( "neo4.com", 935 );

        // and
        StoreId expectedStoreId = new StoreId( 6, 2, 7, 9, 3 );
        when( storeCopyClient.fetchStoreId( fromAddress ) ).thenReturn( expectedStoreId );

        // when
        StoreId storeId = subject.fetchStoreId( fromAddress, namedDatabaseId );

        // then
        assertEquals( expectedStoreId, storeId );
    }

    @Test
    void retrieveStoreDelegatesToStoreCopyService()
            throws StoreCopyFailedException, CatchupAddressResolutionException
    {
        // given
        StoreId storeId = new StoreId( 92, 5, 12, 7, 32 );
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( Path.of( "." ) );

        // when
        subject.copy( anyAddress, storeId, namedDatabaseId, databaseLayout );

        // then
        ArgumentCaptor<CatchupAddressProvider> argumentCaptor = ArgumentCaptor.forClass( CatchupAddressProvider.class );
        verify( remoteStore ).copy( argumentCaptor.capture(), eq( storeId ), eq( databaseLayout ) );

        //and
        assertEquals( anyAddress, argumentCaptor.getValue().primary( new TestDatabaseIdRepository().defaultDatabase() ) );
    }
}
