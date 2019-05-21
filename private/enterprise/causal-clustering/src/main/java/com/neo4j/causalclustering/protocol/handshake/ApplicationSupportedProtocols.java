/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.ApplicationProtocolVersion;
import com.neo4j.causalclustering.protocol.Protocol;

import java.util.List;

public class ApplicationSupportedProtocols extends SupportedProtocols<ApplicationProtocolVersion,Protocol.ApplicationProtocol>
{
    public ApplicationSupportedProtocols( Protocol.Category<Protocol.ApplicationProtocol> category, List<ApplicationProtocolVersion> versions )
    {
        super( category, versions );
    }
}
