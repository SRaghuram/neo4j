/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.error;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.logging.LogProvider;

public class UnavailableDatabaseHandler<T extends CatchupProtocolMessage.WithDatabaseId> extends ErrorReportingHandler<T>
{
    private final AvailabilityGuard availabilityGuard;

    public UnavailableDatabaseHandler( Class<T> messageType, CatchupServerProtocol protocol, AvailabilityGuard availabilityGuard, LogProvider logProvider )
    {
        super( messageType, protocol, logProvider );
        this.availabilityGuard = availabilityGuard;
    }

    @Override
    CatchupErrorResponse newErrorResponse( T request )
    {
        var databaseStatus = availabilityGuard.isShutdown() ? "shutdown" : "unavailable";

        return new CatchupErrorResponse( CatchupResult.E_STORE_UNAVAILABLE,
                String.format( "CatchupRequest %s refused as intended database %s is %s", request, request.databaseId(), databaseStatus ) );
    }
}
