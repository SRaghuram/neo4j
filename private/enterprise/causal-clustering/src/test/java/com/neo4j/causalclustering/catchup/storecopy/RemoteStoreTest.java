/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchUpClientException;
import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.identity.StoreId;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.StorageEngineFactory.selectStorageEngine;

public class RemoteStoreTest
{
    @Test
    public void shouldCopyStoreFilesAndPullTransactions() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        TxPullClient txPullClient = mock( TxPullClient.class );
        when( txPullClient.pullTransactions( any(), any(), anyLong(), any() ) )
                .thenReturn( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, 13 ) );
        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null, storeCopyClient, txPullClient, factory( writer ), Config.defaults(), new Monitors(), storageEngine() );

        // when
        AdvertisedSocketAddress localhost = new AdvertisedSocketAddress( "127.0.0.1", 1234 );
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( localhost );
        remoteStore.copy( catchupAddressProvider, storeId, DatabaseLayout.of( new File( "destination" ) ), true );

        // then
        verify( storeCopyClient ).copyStoreFiles( eq( catchupAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(), any() );
        verify( txPullClient ).pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any() );
    }

    @Test
    public void shouldSetLastPulledTransactionId() throws Exception
    {
        // given
        long lastFlushedTxId = 12;
        StoreId wantedStoreId = new StoreId( 1, 2, 3, 4 );
        AdvertisedSocketAddress localhost = new AdvertisedSocketAddress( "127.0.0.1", 1234 );
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( localhost );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( catchupAddressProvider ), eq( wantedStoreId ), any( StoreFileStreamProvider.class ), any(), any() ) )
                .thenReturn( lastFlushedTxId );

        TxPullClient txPullClient = mock( TxPullClient.class );
        when( txPullClient.pullTransactions( eq( localhost ), eq( wantedStoreId ), anyLong(), any() ) )
                .thenReturn( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, 13 ) );

        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null, storeCopyClient, txPullClient, factory( writer ), Config.defaults(), new Monitors(), storageEngine() );

        // when
        remoteStore.copy( catchupAddressProvider, wantedStoreId, DatabaseLayout.of( new File( "destination" ) ), true );

        // then
        long previousTxId = lastFlushedTxId - 1; // the interface is defined as asking for the one preceding
        verify( txPullClient ).pullTransactions( eq( localhost ), eq( wantedStoreId ), eq( previousTxId ),
                any() );
    }

    @Test
    public void shouldCloseDownTxLogWriterIfTxStreamingFails() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        TxPullClient txPullClient = mock( TxPullClient.class );
        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( null );

        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null, storeCopyClient, txPullClient, factory( writer ), Config.defaults(), new Monitors(), storageEngine() );

        doThrow( CatchUpClientException.class ).when( txPullClient )
                .pullTransactions( isNull(), eq( storeId ), anyLong(), any() );

        // when
        try
        {
            remoteStore.copy( catchupAddressProvider, storeId, DatabaseLayout.of( new File( "." ) ), true );
        }
        catch ( StoreCopyFailedException e )
        {
            // expected
        }

        // then
        verify( writer ).close();
    }

    private StorageEngineFactory storageEngine()
    {
        return selectStorageEngine( Service.load( StorageEngineFactory.class ) );
    }

    private static TransactionLogCatchUpFactory factory( TransactionLogCatchUpWriter writer ) throws IOException
    {
        TransactionLogCatchUpFactory factory = mock( TransactionLogCatchUpFactory.class );
        when( factory.create( any(), any( FileSystemAbstraction.class ), isNull(), any( Config.class ),
                any( LogProvider.class ), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean() ) ).thenReturn( writer );
        return factory;
    }
}
