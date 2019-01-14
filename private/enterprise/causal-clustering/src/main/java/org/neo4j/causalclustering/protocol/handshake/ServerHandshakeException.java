/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

public class ServerHandshakeException extends Exception
{
    public ServerHandshakeException( String message, ProtocolStack.Builder protocolStackBuilder )
    {
        super( message + " Negotiated protocols: " + protocolStackBuilder );
    }
}
