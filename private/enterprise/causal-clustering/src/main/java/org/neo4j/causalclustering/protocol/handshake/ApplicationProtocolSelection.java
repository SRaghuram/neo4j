/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Set;

import org.neo4j.causalclustering.protocol.Protocol;

public class ApplicationProtocolSelection extends ProtocolSelection<Integer,Protocol.ApplicationProtocol>
{
    public ApplicationProtocolSelection( String identifier, Set<Integer> versions )
    {
        super( identifier, versions );
    }
}
