/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.PerDatabaseCatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.common.LocalDatabase;
import com.neo4j.causalclustering.identity.StoreId;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

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
    boolean bringUpToDate( LocalDatabase database, AdvertisedSocketAddress primaryAddress, CatchupAddressProvider addressProvider )
            throws IOException, DatabaseShutdownException
    {
        PerDatabaseCatchupComponents components = getCatchupComponents( database.databaseName() );
        Optional<StoreId> validStoreId = validateStoreId( database, components.remoteStore(), primaryAddress );

        if ( !validStoreId.isPresent() )
        {
            return false;
        }

        if ( !database.isEmpty() )
        {
            if ( tryCatchup( database, addressProvider, components.remoteStore() ) )
            {
                return true;
            }
            else
            {
                database.delete();
            }
        }

        return replaceWithStore( validStoreId.get(), addressProvider, components.storeCopyProcess() );
    }

    /**
     * Returns a store ID which is to be considered locally valid and used in subsequent requests. A store ID
     * is valid either because it matches between the local and the upstream or because the local database
     * is empty anyway so it should be overridden by the remote store ID when the remote database is copied.
     */
    private Optional<StoreId> validateStoreId( LocalDatabase localDatabase, RemoteStore remoteStore, AdvertisedSocketAddress address ) throws IOException
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

        if ( !localDatabase.isEmpty() && !remoteStoreId.equals( localDatabase.storeId() ) )
        {
            log.error( "Store copy failed due to store ID mismatch" );
            return Optional.empty();
        }
        return Optional.of( remoteStoreId );
    }

    /**
     * @return true if catchup was successful.
     */
    private boolean tryCatchup( LocalDatabase database, CatchupAddressProvider addressProvider, RemoteStore remoteStore ) throws IOException
    {
        try
        {
            remoteStore.tryCatchingUp( addressProvider, database.storeId(), database.databaseLayout(), false, false );
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

    private PerDatabaseCatchupComponents getCatchupComponents( String databaseName )
    {
        return componentsRepo.componentsFor( databaseName ).orElseThrow(
                () -> new IllegalStateException( String.format( "There are no catchup components for the database %s.", databaseName ) ) );
    }
}
