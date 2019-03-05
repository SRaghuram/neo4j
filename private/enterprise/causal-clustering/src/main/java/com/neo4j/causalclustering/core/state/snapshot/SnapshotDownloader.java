/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV2;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV1;

public class SnapshotDownloader
{
    private final CatchupClientFactory catchupClientFactory;
    private final Log log;

    public SnapshotDownloader( LogProvider logProvider, CatchupClientFactory catchupClientFactory )
    {
        this.log = logProvider.getLog( getClass() );
        this.catchupClientFactory = catchupClientFactory;
    }

    Optional<CoreSnapshot> getCoreSnapshot( String databaseName, AdvertisedSocketAddress address )
    {
        log.info( "Downloading snapshot from core server at %s", address );

        CoreSnapshot coreSnapshot;
        try
        {
            VersionedCatchupClients client = catchupClientFactory.getClient( address, log );
            CatchupResponseAdaptor<CoreSnapshot> responseHandler = new CatchupResponseAdaptor<CoreSnapshot>()
            {
                @Override
                public void onCoreSnapshot( CompletableFuture<CoreSnapshot> signal, CoreSnapshot response )
                {
                    signal.complete( response );
                }
            };

            coreSnapshot = client
                    .v1( CatchupClientV1::getCoreSnapshot )
                    .v2( CatchupClientV2::getCoreSnapshot )
                    .v3( c -> c.getCoreSnapshot( databaseName ) )
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
