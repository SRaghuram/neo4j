/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Set;

public class ApplicationProtocolRequest extends BaseProtocolRequest<Integer>
{
    ApplicationProtocolRequest( String protocolName, Set<Integer> versions )
    {
        super( protocolName, versions );
    }

    @Override
    public void dispatch( ServerMessageHandler handler )
    {
        handler.handle( this );
    }
}
