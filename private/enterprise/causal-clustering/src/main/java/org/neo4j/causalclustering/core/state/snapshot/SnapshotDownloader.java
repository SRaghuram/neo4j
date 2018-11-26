/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchupClientFactory;
import org.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients;
import org.neo4j.helpers.AdvertisedSocketAddress;
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

    Optional<CoreSnapshot> getCoreSnapshot( AdvertisedSocketAddress address )
    {
        log.info( "Downloading snapshot from core server at %s", address );

        CoreSnapshot coreSnapshot;
        try
        {
            VersionedCatchupClients client = catchupClientFactory.getClient( address );
            CatchupResponseAdaptor<CoreSnapshot> responseHandler = new CatchupResponseAdaptor<CoreSnapshot>()
            {
                @Override
                public void onCoreSnapshot( CompletableFuture<CoreSnapshot> signal, CoreSnapshot response )
                {
                    signal.complete( response );
                }
            };

            coreSnapshot = client
                    .any( VersionedCatchupClients.CatchupClientCommon::getCoreSnapshot )
                    .withResponseHandler( responseHandler )
                    .request( log );
        }
        catch ( Exception e )
        {
            log.warn( "Store copy failed", e );
            return Optional.empty();
        }
        return Optional.of( coreSnapshot );
    }
}
