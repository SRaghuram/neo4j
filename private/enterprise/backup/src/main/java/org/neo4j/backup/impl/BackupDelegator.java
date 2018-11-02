/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Simplifies the process of performing a backup over the transaction protocol by wrapping all the necessary classes
 * and delegating methods to the correct instances.
 */
class BackupDelegator extends LifecycleAdapter
{
    private final RemoteStore remoteStore;
    private final CatchUpClient catchUpClient;
    private final StoreCopyClient storeCopyClient;

    BackupDelegator( RemoteStore remoteStore, CatchUpClient catchUpClient, StoreCopyClient storeCopyClient )
    {
        this.remoteStore = remoteStore;
        this.catchUpClient = catchUpClient;
        this.storeCopyClient = storeCopyClient;
    }

    void copy( AdvertisedSocketAddress fromAddress, StoreId expectedStoreId, DatabaseLayout databaseLayout ) throws StoreCopyFailedException
    {
        remoteStore.copy( new CatchupAddressProvider.SingleAddressProvider( fromAddress ), expectedStoreId, databaseLayout, true );
    }

    CatchupResult tryCatchingUp( AdvertisedSocketAddress fromAddress, StoreId expectedStoreId, DatabaseLayout databaseLayout ) throws StoreCopyFailedException
    {
        try
        {
            return remoteStore.tryCatchingUp( fromAddress, expectedStoreId, databaseLayout, true, true );
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

    public StoreId fetchStoreId( AdvertisedSocketAddress fromAddress ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( fromAddress );
    }
}
