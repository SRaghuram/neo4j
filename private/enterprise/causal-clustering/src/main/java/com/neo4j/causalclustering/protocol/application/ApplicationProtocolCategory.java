/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.application;

import com.neo4j.causalclustering.protocol.Protocol;

public enum ApplicationProtocolCategory implements Protocol.Category<ApplicationProtocol>
{
    RAFT,
    CATCHUP;

    @Override
    public String canonicalName()
    {
        return name().toLowerCase();
    }
}
