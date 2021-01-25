/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.configuration.ApplicationProtocolVersion;

import java.util.Set;

public class ApplicationProtocolRequest extends BaseProtocolRequest<ApplicationProtocolVersion>
{
    static final int MESSAGE_CODE = 1;

    ApplicationProtocolRequest( String protocolName, Set<ApplicationProtocolVersion> versions )
    {
        super( protocolName, versions );
    }

    @Override
    public void dispatch( ServerMessageHandler handler )
    {
        handler.handle( this );
    }
}
