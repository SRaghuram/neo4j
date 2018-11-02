/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

public interface ClientMessageHandler
{
    void handle( InitialMagicMessage magicMessage );

    void handle( ApplicationProtocolResponse applicationProtocolResponse );

    void handle( ModifierProtocolResponse modifierProtocolResponse );

    void handle( SwitchOverResponse switchOverResponse );
}
