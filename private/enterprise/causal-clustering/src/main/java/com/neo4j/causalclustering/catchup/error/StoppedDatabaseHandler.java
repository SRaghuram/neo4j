/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.error;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import org.neo4j.logging.LogProvider;

public class StoppedDatabaseHandler<T extends CatchupProtocolMessage> extends ErrorReportingHandler<T>
{
    public StoppedDatabaseHandler( Class<T> messageType, CatchupServerProtocol protocol, LogProvider logProvider )
    {
        super( messageType, protocol, logProvider );
    }

    @Override
    CatchupErrorResponse newErrorResponse( T request )
    {
        return new CatchupErrorResponse( CatchupResult.E_STORE_UNAVAILABLE,
                String.format( "CatchupRequest %s refused as intended database %s is stopped", request, request.databaseId().name() ) );
    }
}
