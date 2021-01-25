/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import org.neo4j.configuration.helpers.SocketAddress;

public interface ServerHandshakeFinishedEvent
{
    class Created
    {
        public final SocketAddress advertisedSocketAddress;
        public final ProtocolStack protocolStack;

        public Created( SocketAddress advertisedSocketAddress, ProtocolStack protocolStack )
        {
            this.advertisedSocketAddress = advertisedSocketAddress;
            this.protocolStack = protocolStack;
        }
    }

    class Closed
    {
        public final SocketAddress advertisedSocketAddress;

        public Closed( SocketAddress advertisedSocketAddress )
        {
            this.advertisedSocketAddress = advertisedSocketAddress;
        }
    }
}
