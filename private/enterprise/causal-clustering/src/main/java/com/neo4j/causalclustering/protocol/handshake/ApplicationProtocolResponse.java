/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

public class ApplicationProtocolResponse extends BaseProtocolResponse<Integer>
{
    public static final ApplicationProtocolResponse NO_PROTOCOL = new ApplicationProtocolResponse( StatusCode.FAILURE, "", 0 );

    ApplicationProtocolResponse( StatusCode statusCode, String protocolName, int version )
    {
        super( statusCode, protocolName, version );
    }

    @Override
    public void dispatch( ClientMessageHandler handler )
    {
        handler.handle( this );
    }
}
