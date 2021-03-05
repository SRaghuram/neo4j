/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.helper.StoreValidation;

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
            StoreId remoteStoreId = catchupComponents.remoteStore().getStoreId( primaryAddress );
            if ( context.hasStore() )
            {
                if ( tryCatchup( context, addressProvider, catchupComponents, remoteStoreId, primaryAddress ) )
                {
                    return;
                }
                context.delete();
            }
            replaceStore( addressProvider, catchupComponents, remoteStoreId );
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
    private boolean tryCatchup( StoreDownloadContext context, CatchupAddressProvider addressProvider, CatchupComponents catchupComponents,
            StoreId remoteStoreId, SocketAddress address )
            throws IOException, DatabaseShutdownException, SnapshotFailedException, StoreIdDownloadFailedException
    {
        try
        {
            StoreId localStoreId = context.storeId();
            if ( !StoreValidation.validRemoteToUseTransactionsFrom( localStoreId, remoteStoreId, context.kernelDatabase().getStorageEngineFactory() ) )
            {
                throw new SnapshotFailedException( "Store copy failed due to store ID mismatch. There are database operating with different store ids." +
                        " Received a different store ID from " + address, SnapshotFailedException.Status.UNRECOVERABLE );
            }
            log.info( "Begin trying to catch up store" );
            catchupComponents.remoteStore().tryCatchingUp( addressProvider, localStoreId, context.databaseLayout(), false );
            catchupComponents.copiedStoreRecovery().recoverCopiedStore( context.kernelDatabase().getConfig(), context.databaseLayout() );
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
