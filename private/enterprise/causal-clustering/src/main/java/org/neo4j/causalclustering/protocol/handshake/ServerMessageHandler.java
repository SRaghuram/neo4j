/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

public interface ServerMessageHandler
{
    void handle( InitialMagicMessage magicMessage );

    void handle( ApplicationProtocolRequest applicationProtocolRequest );

    void handle( ModifierProtocolRequest modifierProtocolRequest );

    void handle( SwitchOverRequest switchOverRequest );
}
