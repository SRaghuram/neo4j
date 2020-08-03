/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

public class StubClusteringIdentityModule extends ClusteringIdentityModule
{
    private final MemberId memberId;

    public StubClusteringIdentityModule()
    {
        memberId = IdFactory.randomMemberId();
    }

    @Override
    public ServerId myself()
    {
        return memberId;
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
}
