/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.member.DiscoveryMember;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

public class TestDiscoveryMember implements DiscoveryMember
{
    private final MemberId id;
    private final Set<DatabaseId> startedDatabases;

    public TestDiscoveryMember()
    {
        this( new MemberId( UUID.randomUUID() ) );
    }

    public TestDiscoveryMember( MemberId memberId )
    {
        this( memberId, Set.of( new TestDatabaseIdRepository().defaultDatabase() ) );
    }

    public TestDiscoveryMember( MemberId id, Set<DatabaseId> startedDatabases )
    {
        this.id = id;
        this.startedDatabases = startedDatabases;
    }

    @Override
    public MemberId id()
    {
        return id;
    }

    @Override
    public Set<DatabaseId> startedDatabases()
    {
        return startedDatabases;
    }
}
