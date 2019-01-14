/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.List;

import org.neo4j.causalclustering.protocol.Protocol;

public class ApplicationSupportedProtocols extends SupportedProtocols<Integer,Protocol.ApplicationProtocol>
{
    public ApplicationSupportedProtocols( Protocol.Category<Protocol.ApplicationProtocol> category, List<Integer> versions )
    {
        super( category, versions );
    }
}
