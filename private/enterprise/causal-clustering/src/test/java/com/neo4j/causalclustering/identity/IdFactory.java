/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;

public final class IdFactory
{
    private IdFactory()
    {
    }

    public static RaftGroupId randomRaftId()
    {
        return new RaftGroupId( UUID.randomUUID() );
    }

    public static RaftMemberId randomRaftMemberId()
    {
        return new RaftMemberId( UUID.randomUUID() );
    }

    public static ServerId randomServerId()
    {
        return new ServerId( UUID.randomUUID() );
    }
}
