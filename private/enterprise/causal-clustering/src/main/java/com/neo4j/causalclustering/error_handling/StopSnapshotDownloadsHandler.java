/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;

import org.neo4j.logging.Log;

public class StopSnapshotDownloadsHandler implements DatabasePanicEventHandler
{
    private final CoreDownloaderService coreDownloaderService;
    private final Log log;

    public StopSnapshotDownloadsHandler( CoreDownloaderService coreDownloaderService, Log log )
    {
        this.coreDownloaderService = coreDownloaderService;
        this.log = log;
    }

    @Override
    public void onPanic( DatabasePanicEvent panic )
    {
        try
        {
            coreDownloaderService.stop();
        }
        catch ( Exception e )
        {
            log.error( "Error while attempting to stop CoreDownloaderService due to panic: " + e.getMessage() );
        }
    }
}
