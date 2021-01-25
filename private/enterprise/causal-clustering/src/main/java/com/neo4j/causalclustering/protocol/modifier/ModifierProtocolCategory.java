/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.modifier;

import com.neo4j.causalclustering.protocol.Protocol;

public enum ModifierProtocolCategory implements Protocol.Category<ModifierProtocol>
{
    COMPRESSION,
    // Need a second Category for testing purposes.
    GRATUITOUS_OBFUSCATION;

    @Override
    public String canonicalName()
    {
        return name().toLowerCase();
    }
}
