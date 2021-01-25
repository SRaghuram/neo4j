/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

public class ModifierProtocolResponse extends BaseProtocolResponse<String>
{
    static final int MESSAGE_CODE = 1;

    ModifierProtocolResponse( StatusCode statusCode, String protocolName, String implementation )
    {
        super( statusCode, protocolName, implementation );
    }

    static ModifierProtocolResponse failure( String protocolName )
    {
        return new ModifierProtocolResponse( StatusCode.FAILURE, protocolName, "" );
    }

    @Override
    public void dispatch( ClientMessageHandler handler )
    {
        handler.handle( this );
    }
}
