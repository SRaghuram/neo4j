/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.configuration.ApplicationProtocolVersion;

public class ApplicationProtocolResponse extends BaseProtocolResponse<ApplicationProtocolVersion>
{
    static final int MESSAGE_CODE = 0;

    static final ApplicationProtocolResponse NO_PROTOCOL = new ApplicationProtocolResponse( StatusCode.FAILURE, "", new ApplicationProtocolVersion( 0, 0 ) );

    ApplicationProtocolResponse( StatusCode statusCode, String protocolName, ApplicationProtocolVersion version )
    {
        super( statusCode, protocolName, version );
    }

    @Override
    public void dispatch( ClientMessageHandler handler )
    {
        handler.handle( this );
    }
}
