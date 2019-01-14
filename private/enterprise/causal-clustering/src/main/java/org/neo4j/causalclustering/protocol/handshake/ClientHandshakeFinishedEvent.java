/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

public interface ClientHandshakeFinishedEvent
{
    class Success implements ClientHandshakeFinishedEvent
    {
        private final ProtocolStack protocolStack;

        public Success( ProtocolStack protocolStack )
        {
            this.protocolStack = protocolStack;
        }

        public ProtocolStack protocolStack()
        {
            return protocolStack;
        }
    }

    class Failure implements ClientHandshakeFinishedEvent
    {
        private Failure()
        {
        }

        private static Failure instance = new Failure();

        public static Failure instance()
        {
            return instance;
        }
    }
}
