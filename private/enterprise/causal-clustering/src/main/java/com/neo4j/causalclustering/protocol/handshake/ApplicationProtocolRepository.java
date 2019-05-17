/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.ApplicationProtocolVersion;
import com.neo4j.causalclustering.protocol.Protocol;

public class ApplicationProtocolRepository extends ProtocolRepository<ApplicationProtocolVersion,Protocol.ApplicationProtocol>
{
    private final ApplicationSupportedProtocols supportedProtocol;

    public ApplicationProtocolRepository( Protocol.ApplicationProtocol[] protocols, ApplicationSupportedProtocols supportedProtocol )
    {
        super( protocols, ignored -> versionNumberComparator(), ApplicationProtocolSelection::new );
        this.supportedProtocol = supportedProtocol;
    }

    public ApplicationSupportedProtocols supportedProtocol()
    {
        return supportedProtocol;
    }
}
