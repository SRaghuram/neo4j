/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

public interface ServerMessageHandler
{
    void handle( ApplicationProtocolRequest applicationProtocolRequest );

    void handle( ModifierProtocolRequest modifierProtocolRequest );

    void handle( SwitchOverRequest switchOverRequest );
}
