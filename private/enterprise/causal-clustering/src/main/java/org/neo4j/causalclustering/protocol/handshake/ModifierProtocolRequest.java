/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Set;

public class ModifierProtocolRequest extends BaseProtocolRequest<String>
{
    ModifierProtocolRequest( String protocolName, Set<String> versions )
    {
        super( protocolName, versions );
    }

    @Override
    public void dispatch( ServerMessageHandler handler )
    {
        handler.handle( this );
    }
}
