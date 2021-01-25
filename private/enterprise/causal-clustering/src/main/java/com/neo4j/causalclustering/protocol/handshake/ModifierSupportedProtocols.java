/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;

import java.util.List;

public class ModifierSupportedProtocols extends SupportedProtocols<String,ModifierProtocol>
{
    public ModifierSupportedProtocols( Protocol.Category<ModifierProtocol> category, List<String> versions )
    {
        super( category, versions );
    }
}
