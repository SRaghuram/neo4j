/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponse;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

/**
 * Simplifies the process of performing a backup over the transaction protocol by wrapping all the necessary classes
 * and delegating methods to the correct instances.
 */
class BackupDelegator extends LifecycleAdapter
{
    private final Function<DatabaseId,RemoteStore> remoteStore;
    private final Function<DatabaseId,StoreCopyClient> storeCopyClient;
    private final CatchupClientFactory catchUpClient;
    private final LogProvider logProvider;

    BackupDelegator( Function<DatabaseId,RemoteStore> remoteStore, Function<DatabaseId,StoreCopyClient> storeCopyClient, CatchupClientFactory catchUpClient,
            LogProvider logProvider )
    {
        this.remoteStore = remoteStore;
        this.storeCopyClient = storeCopyClient;
        this.catchUpClient = catchUpClient;
        this.logProvider = logProvider;
    }

    void copy( SocketAddress fromAddress, StoreId expectedStoreId, DatabaseId databaseId, DatabaseLayout databaseLayout ) throws StoreCopyFailedException
    {
        remoteStore.apply( databaseId ).copy( new SingleAddressProvider( fromAddress ), expectedStoreId, databaseLayout, true );
    }

    void tryCatchingUp( SocketAddress fromAddress, StoreId expectedStoreId, DatabaseId databaseId, DatabaseLayout databaseLayout )
            throws StoreCopyFailedException
    {
        try
        {
            remoteStore.apply( databaseId ).tryCatchingUp( new SingleAddressProvider( fromAddress ), expectedStoreId, databaseLayout, true, true );
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    @Override
    public void start()
    {
        catchUpClient.start();
    }

    @Override
    public void stop()
    {
        catchUpClient.stop();
    }

    public StoreId fetchStoreId( SocketAddress fromAddress, DatabaseId databaseId ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.apply( databaseId ).fetchStoreId( fromAddress );
    }

    public DatabaseId fetchDatabaseId( SocketAddress fromAddress, String databaseName ) throws DatabaseIdDownloadFailedException
    {
        var copyWithDatabaseId = new CatchupResponseAdaptor<DatabaseId>()
        {
            @Override
            public void onGetDatabaseIdResponse( CompletableFuture<DatabaseId> signal, GetDatabaseIdResponse response )
            {
                signal.complete( response.databaseId() );
            }
        };

        try
        {
            return catchUpClient.getClient( fromAddress, logProvider.getLog( getClass() ) )
                    .v3( client -> client.getDatabaseId( databaseName ) )
                    .withResponseHandler( copyWithDatabaseId )
                    .request();
        }
        catch ( Exception e )
        {
            throw new DatabaseIdDownloadFailedException( e );
        }
    }
}
