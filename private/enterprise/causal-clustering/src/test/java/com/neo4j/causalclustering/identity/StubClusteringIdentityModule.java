/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

public class StubClusteringIdentityModule extends ClusteringIdentityModule
{
    private final MemberId memberId;
    private final RaftMemberId raftMemberId;

    public StubClusteringIdentityModule()
    {
        memberId = IdFactory.randomMemberId();
        raftMemberId = RaftMemberId.from( memberId );
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
    public RaftMemberId memberId( NamedDatabaseId namedDatabaseId )
    {
        return raftMemberId;
    }
}
