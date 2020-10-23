/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponse;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataResponse;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

/**
 * Simplifies the process of performing a backup over the transaction protocol by wrapping all the necessary classes
 * and delegating methods to the correct instances.
 */
class BackupDelegator extends LifecycleAdapter
{
    private final Function<NamedDatabaseId,RemoteStore> remoteStore;
    private final Function<NamedDatabaseId,StoreCopyClient> storeCopyClient;
    private final CatchupClientFactory catchUpClient;
    private final LogProvider logProvider;

    BackupDelegator( Function<NamedDatabaseId,RemoteStore> remoteStore, Function<NamedDatabaseId,StoreCopyClient> storeCopyClient,
            CatchupClientFactory catchUpClient, LogProvider logProvider )
    {
        this.remoteStore = remoteStore;
        this.storeCopyClient = storeCopyClient;
        this.catchUpClient = catchUpClient;
        this.logProvider = logProvider;
    }

    void copy( SocketAddress fromAddress, StoreId expectedStoreId, NamedDatabaseId namedDatabaseId, DatabaseLayout databaseLayout )
            throws StoreCopyFailedException
    {
        remoteStore.apply( namedDatabaseId ).copy( new SingleAddressProvider( fromAddress ), expectedStoreId, databaseLayout );
    }

    void tryCatchingUp( SocketAddress fromAddress, StoreId expectedStoreId, NamedDatabaseId namedDatabaseId, DatabaseLayout databaseLayout )
            throws StoreCopyFailedException
    {
        try
        {
            remoteStore.apply( namedDatabaseId ).tryCatchingUp( new SingleAddressProvider( fromAddress ), expectedStoreId, databaseLayout, true );
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

    public StoreId fetchStoreId( SocketAddress fromAddress, NamedDatabaseId namedDatabaseId ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.apply( namedDatabaseId ).fetchStoreId( fromAddress );
    }

    public NamedDatabaseId fetchDatabaseId( SocketAddress fromAddress, String databaseName ) throws DatabaseIdDownloadFailedException
    {
        var copyWithDatabaseId = new CatchupResponseAdaptor<NamedDatabaseId>()
        {
            @Override
            public void onGetDatabaseIdResponse( CompletableFuture<NamedDatabaseId> signal, GetDatabaseIdResponse response )
            {
                signal.complete( DatabaseIdFactory.from( databaseName, response.databaseId().uuid() ) );
            }
        };

        try
        {
            return catchUpClient.getClient( fromAddress, logProvider.getLog( getClass() ) )
                                .v3( client -> client.getDatabaseId( databaseName ) )
                                .v4( client -> client.getDatabaseId( databaseName ) )
                                .v5( client -> client.getDatabaseId( databaseName ) )
                                .withResponseHandler( copyWithDatabaseId )
                                .request();
        }
        catch ( Exception e )
        {
            throw new DatabaseIdDownloadFailedException( e );
        }
    }

    public Set<NamedDatabaseId> getAllDatabaseIds( SocketAddress fromAddress ) throws BackupExecutionException
    {
        final CatchupResponseAdaptor<GetAllDatabaseIdsResponse> responseHandler = new CatchupResponseAdaptor<GetAllDatabaseIdsResponse>()
        {
            @Override
            public void onGetAllDatabaseIdsResponse( CompletableFuture<GetAllDatabaseIdsResponse> signal, GetAllDatabaseIdsResponse response )
            {
                signal.complete( response );
            }
        };
        try
        {
            return catchUpClient.getClient( fromAddress, logProvider.getLog( getClass() ) )
                                .v3( VersionedCatchupClients.CatchupClientV3::getAllDatabaseIds )
                                .v4( VersionedCatchupClients.CatchupClientV4::getAllDatabaseIds )
                                .v5( VersionedCatchupClients.CatchupClientV5::getAllDatabaseIds )
                                .withResponseHandler( responseHandler )
                                .request()
                                .databaseIds();
        }
        catch ( Exception ex )
        {
            logProvider.getLog( getClass() ).error( "Error in execution get all database ids", ex );
            throw new BackupExecutionException( ex );
        }
    }

    public List<String> getMetadata( SocketAddress fromAddress, String databaseName, String includeMetadata )
    {
        final CatchupResponseAdaptor<GetMetadataResponse> responseHandler = new CatchupResponseAdaptor<>()
        {
            @Override
            public void onGetMetadataResponse( CompletableFuture<GetMetadataResponse> signal, GetMetadataResponse response )
            {
                signal.complete( response );
            }
        };

        try
        {
            return catchUpClient.getClient( fromAddress, logProvider.getLog( getClass() ) )
                                .v3( client -> client.getMetadata( databaseName, includeMetadata ) )
                                .v4( client -> client.getMetadata( databaseName, includeMetadata ) )
                                .v5( client -> client.getMetadata( databaseName, includeMetadata ) )
                                .withResponseHandler( responseHandler )
                                .request()
                    .commands;
        }
        catch ( Exception ex )
        {
            logProvider.getLog( getClass() ).error( "Error in execution of get metadata", ex );
            return List.of();
        }
    }
}
