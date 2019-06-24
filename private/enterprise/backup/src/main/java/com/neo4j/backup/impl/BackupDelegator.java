/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;

import java.io.IOException;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.StoreId;

/**
 * Simplifies the process of performing a backup over the transaction protocol by wrapping all the necessary classes
 * and delegating methods to the correct instances.
 */
class BackupDelegator extends LifecycleAdapter
{
    private final RemoteStore remoteStore;
    private final CatchupClientFactory catchUpClient;
    private final StoreCopyClient storeCopyClient;

    BackupDelegator( RemoteStore remoteStore, CatchupClientFactory catchUpClient, StoreCopyClient storeCopyClient )
    {
        this.remoteStore = remoteStore;
        this.catchUpClient = catchUpClient;
        this.storeCopyClient = storeCopyClient;
    }

    void copy( SocketAddress fromAddress, StoreId expectedStoreId, DatabaseLayout databaseLayout ) throws StoreCopyFailedException
    {
        remoteStore.copy( new SingleAddressProvider( fromAddress ), expectedStoreId, databaseLayout, true );
    }

    void tryCatchingUp( SocketAddress fromAddress, StoreId expectedStoreId, DatabaseLayout databaseLayout ) throws StoreCopyFailedException
    {
        try
        {
            remoteStore.tryCatchingUp( new SingleAddressProvider( fromAddress ), expectedStoreId, databaseLayout, true, true );
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

    public StoreId fetchStoreId( SocketAddress fromAddress ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( fromAddress );
    }
}
