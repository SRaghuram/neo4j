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
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;

import java.io.IOException;

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
     * @throws SnapshotFailedException if unable to bring store up do date. The {@link SnapshotFailedException.Status} of the exception may vary.
     */
    void bringUpToDate( StoreDownloadContext context, SocketAddress primaryAddress, CatchupAddressProvider addressProvider ) throws SnapshotFailedException
    {
        try
        {
            var catchupComponents = getCatchupComponents( context.databaseId() );
            var storeId = validateStoreId( context, catchupComponents.remoteStore(), primaryAddress );
            if ( context.hasStore() )
            {
                if ( tryCatchup( context, addressProvider, catchupComponents.remoteStore() ) )
                {
                    return;
                }
                context.delete();
            }
            replaceStore( addressProvider, catchupComponents, storeId );
        }
        catch ( IOException e )
        {
            throw new SnapshotFailedException( "Unexpected IO error when updating store", SnapshotFailedException.Status.UNRECOVERABLE, e );
        }
        catch ( DatabaseShutdownException e )
        {
            throw new SnapshotFailedException( "Failed to bring store up to date due to it being shutdown", SnapshotFailedException.Status.TERMINAL, e );
        }
        catch ( StoreIdDownloadFailedException | StoreCopyFailedException e )
        {
            throw new SnapshotFailedException( "Failed to bring store up to date", SnapshotFailedException.Status.RETRYABLE, e );
        }
    }

    /**
     * Returns a store ID which is to be considered locally valid and used in subsequent requests. A store ID is valid either because it matches between the
     * local and the upstream or because the local database is empty anyway so it should be overridden by the remote store ID when the remote database is
     * copied.
     *
     * @return {@link StoreId} returns remote store id if it matches the local store id or if local store id is empty.
     * @throws SnapshotFailedException        with unrecoverable error if remote store id does not match local.
     * @throws StoreIdDownloadFailedException if unable to download remote store id.
     */
    private StoreId validateStoreId( StoreDownloadContext context, RemoteStore remoteStore, SocketAddress address )
            throws StoreIdDownloadFailedException, SnapshotFailedException
    {
        var remoteStoreId = remoteStore.getStoreId( address );

        if ( context.hasStore() && !remoteStoreId.equals( context.storeId() ) )
        {
            throw new SnapshotFailedException(
                    "Store copy failed due to store ID mismatch. There are database operating with different store ids.",
                    SnapshotFailedException.Status.RETRYABLE );
        }
        return remoteStoreId;
    }

    private void replaceStore( CatchupAddressProvider addressProvider, CatchupComponents catchupComponents, StoreId storeId )
            throws IOException, StoreCopyFailedException, DatabaseShutdownException
    {
        log.info( "Begin replacing store" );
        catchupComponents.storeCopyProcess().replaceWithStoreFrom( addressProvider, storeId );
        log.info( "Successfully replaced store" );
    }

    /**
     * @return true if catchup was successful.
     */
    private boolean tryCatchup( StoreDownloadContext context, CatchupAddressProvider addressProvider, RemoteStore remoteStore ) throws IOException
    {
        try
        {
            log.info( "Begin trying to catch up store" );
            remoteStore.tryCatchingUp( addressProvider, context.storeId(), context.databaseLayout(), false );
            log.info( "Successfully caught up store" );
            return true;
        }
        catch ( StoreCopyFailedException e )
        {
            log.warn( "Failed to catch up", e );
            return false;
        }
    }

    private CatchupComponents getCatchupComponents( NamedDatabaseId namedDatabaseId )
    {
        return componentsRepo.componentsFor( namedDatabaseId ).orElseThrow(
                () -> new IllegalStateException( String.format( "There are no catchup components for the database %s.", namedDatabaseId ) ) );
    }
}
