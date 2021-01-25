/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;

import java.util.Set;

public class ModifierProtocolSelection extends ProtocolSelection<String,ModifierProtocol>
{
    public ModifierProtocolSelection( String identifier, Set<String> versions )
    {
        super( identifier, versions );
    }
}
