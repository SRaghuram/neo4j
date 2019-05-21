/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class SnapshotDownloader
{
    private final CatchupClientFactory catchupClientFactory;
    private final Log log;

    public SnapshotDownloader( LogProvider logProvider, CatchupClientFactory catchupClientFactory )
    {
        this.log = logProvider.getLog( getClass() );
        this.catchupClientFactory = catchupClientFactory;
    }

    Optional<CoreSnapshot> getCoreSnapshot( DatabaseId databaseId, AdvertisedSocketAddress address )
    {
        log.info( "Downloading snapshot from core server at %s", address );

        CoreSnapshot coreSnapshot;
        try
        {
            VersionedCatchupClients client = catchupClientFactory.getClient( address, log );
            CatchupResponseAdaptor<CoreSnapshot> responseHandler = new CatchupResponseAdaptor<>()
            {
                @Override
                public void onCoreSnapshot( CompletableFuture<CoreSnapshot> signal, CoreSnapshot response )
                {
                    signal.complete( response );
                }
            };

            coreSnapshot = client
                    .v3( c -> c.getCoreSnapshot( databaseId ) )
                    .withResponseHandler( responseHandler )
                    .request();
        }
        catch ( Exception e )
        {
            log.warn( "Store copy failed", e );
            return Optional.empty();
        }
        return Optional.of( coreSnapshot );
    }
}
