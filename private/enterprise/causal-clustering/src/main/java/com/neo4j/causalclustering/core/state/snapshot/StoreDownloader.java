/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

public class StoreDownloader
{
    private final CatchupComponentsRepository componentsRepo;
    private final Log log;

    public StoreDownloader( CatchupComponentsRepository componentsRepo, LogProvider logProvider )
    {
        this.componentsRepo = componentsRepo;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Brings the store up-to-date either by pulling transactions or by doing a store copy.
     *
     * @return true if successful.
     */
    boolean bringUpToDate( StoreDownloadContext context, SocketAddress primaryAddress, CatchupAddressProvider addressProvider )
            throws IOException, DatabaseShutdownException
    {
        CatchupComponents components = getCatchupComponents( context.databaseId() );
        Optional<StoreId> validStoreId = validateStoreId( context, components.remoteStore(), primaryAddress );

        if ( validStoreId.isEmpty() )
        {
            return false;
        }

        if ( !context.isEmpty() )
        {
            if ( tryCatchup( context, addressProvider, components.remoteStore() ) )
            {
                return true;
            }
            else
            {
                context.delete();
            }
        }

        return replaceWithStore( validStoreId.get(), addressProvider, components.storeCopyProcess() );
    }

    /**
     * Returns a store ID which is to be considered locally valid and used in subsequent requests. A store ID
     * is valid either because it matches between the local and the upstream or because the local database
     * is empty anyway so it should be overridden by the remote store ID when the remote database is copied.
     */
    private Optional<StoreId> validateStoreId( StoreDownloadContext context, RemoteStore remoteStore, SocketAddress address )
    {
        StoreId remoteStoreId;
        try
        {
            remoteStoreId = remoteStore.getStoreId( address );
        }
        catch ( StoreIdDownloadFailedException e )
        {
            log.warn( "Store copy failed", e );
            return Optional.empty();
        }

        if ( !context.isEmpty() && !remoteStoreId.equals( context.storeId() ) )
        {
            log.error( "Store copy failed due to store ID mismatch" );
            return Optional.empty();
        }
        return Optional.ofNullable( remoteStoreId );
    }

    /**
     * @return true if catchup was successful.
     */
    private boolean tryCatchup( StoreDownloadContext context, CatchupAddressProvider addressProvider, RemoteStore remoteStore ) throws IOException
    {
        try
        {
            remoteStore.tryCatchingUp( addressProvider, context.storeId(), context.databaseLayout(), false );
            return true;
        }
        catch ( StoreCopyFailedException e )
        {
            log.warn( "Failed to catch up", e );
            return false;
        }
    }

    private boolean replaceWithStore( StoreId remoteStoreId, CatchupAddressProvider addressProvider, StoreCopyProcess storeCopy )
            throws IOException, DatabaseShutdownException
    {
        try
        {
            storeCopy.replaceWithStoreFrom( addressProvider, remoteStoreId );
        }
        catch ( StoreCopyFailedException e )
        {
            log.warn( "Failed to copy and replace store", e );
            return false;
        }
        return true;
    }

    private CatchupComponents getCatchupComponents( NamedDatabaseId namedDatabaseId )
    {
        return componentsRepo.componentsFor( namedDatabaseId ).orElseThrow(
                () -> new IllegalStateException( String.format( "There are no catchup components for the database %s.", namedDatabaseId ) ) );
    }
}
