/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Responsible for bringing the core up-to-date, potentially by copying entire stores etc.
 *
 * This component is invoked when the regular catchup process is insufficient, for example
 * during the first startup when there is nothing to catchup, or if this member has fallen
 * too far behind.
 */
public class CoreDownloader
{
    private final SnapshotDownloader snapshotDownloader;
    private final StoreDownloader storeDownloader;
    private final Log log;

    public CoreDownloader( SnapshotDownloader snapshotDownloader, StoreDownloader storeDownloader, LogProvider logProvider )
    {
        this.snapshotDownloader = snapshotDownloader;
        this.storeDownloader = storeDownloader;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Tries to catchup this instance by downloading a snapshot. A complete snapshot conceptually consists
     * of both the comparatively small state of the cluster state machines as well as the database store.
     * <p>
     * The stores are caught up using two different approaches. If it is possible to catchup by
     * pulling transactions, then this will be sufficient, but if the store is lagging too far behind then
     * a complete store copy will be attempted.
     * <p>
     * The core snapshot must be copied before the stores, because the stores have a dependency on
     * the state of the state machines. The stores will thus be at or ahead of the state machines,
     * in consensus log index, and application of commands will bring them in sync. Any such commands
     * that carry transactions will thus be ignored by the transaction/token state machines, since they
     * are ahead, and the correct decisions for their applicability have already been taken as encapsulated
     * in the copied stores.
     *
     * @param addressProvider Provider of addresses to catchup from.
     * @return the small snapshot if everything went fine and stores could be brought up to sync, otherwise an empty optional.
     * @throws IOException An issue with I/O.
     * @throws DatabaseShutdownException The database is shutting down.
     */
    Optional<CoreSnapshot> downloadSnapshotAndStore( StoreDownloadContext context, CatchupAddressProvider addressProvider )
            throws IOException, DatabaseShutdownException
    {
        Optional<SocketAddress> primaryOpt = lookupPrimary( context.databaseId(), addressProvider );
        if ( primaryOpt.isEmpty() )
        {
            return Optional.empty();
        }
        SocketAddress primaryAddress = primaryOpt.get();

        Optional<CoreSnapshot> coreSnapshot = snapshotDownloader.getCoreSnapshot( context.databaseId(), primaryAddress );
        if ( coreSnapshot.isEmpty() )
        {
            return Optional.empty();
        }

        if ( !storeDownloader.bringUpToDate( context, primaryAddress, addressProvider ) )
        {
            return Optional.empty();
        }

        return coreSnapshot;
    }

    private Optional<SocketAddress> lookupPrimary( DatabaseId databaseId, CatchupAddressProvider addressProvider )
    {
        try
        {
            return Optional.of( addressProvider.primary( databaseId ) );
        }
        catch ( CatchupAddressResolutionException e )
        {
            log.warn( "Store copy failed, as we're unable to find the target catchup address. [Message: %s]", e.getMessage() );
            return Optional.empty();
        }
    }
}
