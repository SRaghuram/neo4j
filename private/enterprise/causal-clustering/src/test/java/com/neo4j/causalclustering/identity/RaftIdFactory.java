/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.util.UUID;

public final class RaftIdFactory
{
    private RaftIdFactory()
    {
    }

    public static RaftId random()
    {
        return new RaftId( UUID.randomUUID() );
    }
}
