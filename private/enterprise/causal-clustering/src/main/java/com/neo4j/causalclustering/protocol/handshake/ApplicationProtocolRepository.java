/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.configuration.ApplicationProtocolVersion;

public class ApplicationProtocolRepository extends ProtocolRepository<ApplicationProtocolVersion,ApplicationProtocol>
{
    private final ApplicationSupportedProtocols supportedProtocol;

    public ApplicationProtocolRepository( ApplicationProtocol[] protocols, ApplicationSupportedProtocols supportedProtocol )
    {
        super( protocols, ignored -> versionNumberComparator(), ApplicationProtocolSelection::new );
        this.supportedProtocol = supportedProtocol;
    }

    ApplicationSupportedProtocols supportedProtocol()
    {
        return supportedProtocol;
    }
}
