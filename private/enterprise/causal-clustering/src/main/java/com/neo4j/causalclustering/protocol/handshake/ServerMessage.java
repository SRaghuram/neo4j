/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

/**
 * Messages to the server, generally requests.
 */
public interface ServerMessage
{
    void dispatch( ServerMessageHandler handler );
}
