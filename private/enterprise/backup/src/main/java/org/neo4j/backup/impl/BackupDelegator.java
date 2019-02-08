/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.identity.StoreId;

import java.io.IOException;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.LifeSupport;

/**
 * Simplifies the process of performing a backup over the transaction protocol by wrapping all the necessary classes
 * and delegating methods to the correct instances.
 */
class BackupDelegator extends LifeSupport
{
    private final RemoteStore remoteStore;
    private final StoreCopyClient storeCopyClient;

    BackupDelegator( RemoteStore remoteStore, StoreCopyClient storeCopyClient )
    {
        this.remoteStore = remoteStore;
        this.storeCopyClient = storeCopyClient;
    }

    void copy( AdvertisedSocketAddress fromAddress, StoreId expectedStoreId, DatabaseLayout databaseLayout ) throws StoreCopyFailedException
    {
        remoteStore.copy( new SingleAddressProvider( fromAddress ), expectedStoreId, databaseLayout, true );
    }

    void tryCatchingUp( AdvertisedSocketAddress fromAddress, StoreId expectedStoreId, DatabaseLayout databaseLayout ) throws StoreCopyFailedException
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

    public StoreId fetchStoreId( AdvertisedSocketAddress fromAddress ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( fromAddress );
    }
}
