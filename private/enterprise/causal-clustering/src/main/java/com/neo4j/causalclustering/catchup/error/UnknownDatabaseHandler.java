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

import org.neo4j.logging.LogProvider;

public class UnknownDatabaseHandler<T extends CatchupProtocolMessage.WithDatabaseId> extends ErrorReportingHandler<T>
{
    public UnknownDatabaseHandler( Class<T> messageType, CatchupServerProtocol protocol, LogProvider logProvider )
    {
        super( messageType, protocol, logProvider );
    }

    @Override
    CatchupErrorResponse newErrorResponse( T request )
    {
        return new CatchupErrorResponse( CatchupResult.E_DATABASE_UNKNOWN,
                String.format( "CatchupRequest %s refused as intended database %s does not exist", request, request.databaseId() ) );
    }
}
