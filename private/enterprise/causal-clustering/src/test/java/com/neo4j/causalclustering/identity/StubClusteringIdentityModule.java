/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

public class StubClusteringIdentityModule implements ClusteringIdentityModule
{
    private final ServerId serverId;
    private final MemberId memberId;

    public StubClusteringIdentityModule()
    {
        var uuid = UUID.randomUUID();
        serverId = new ServerId( uuid );
        memberId = new MemberId( uuid );
    }

    @Override
    public MemberId memberId()
    {
        return memberId;
    }

    @Override
    public MemberId memberId( NamedDatabaseId namedDatabaseId )
    {
        return memberId;
    }

    @Override
    public ServerId myself()
    {
        return serverId;
    }
}
