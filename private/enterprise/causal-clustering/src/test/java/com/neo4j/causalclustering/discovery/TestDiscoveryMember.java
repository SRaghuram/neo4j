/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TestDiscoveryMember implements DiscoveryMember
{
    private final MemberId id;
    private final Set<DatabaseId> databaseIds;

    public TestDiscoveryMember()
    {
        this( randomMemberId() );
    }

    public TestDiscoveryMember( MemberId memberId )
    {
        this( memberId, Set.of( new DatabaseId( DEFAULT_DATABASE_NAME ) ) );
    }

    public TestDiscoveryMember( Set<DatabaseId> databaseIds )
    {
        this( randomMemberId(), databaseIds );
    }

    public TestDiscoveryMember( MemberId id, Set<DatabaseId> databaseIds )
    {
        this.id = id;
        this.databaseIds = databaseIds;
    }

    @Override
    public MemberId id()
    {
        return id;
    }

    @Override
    public Set<DatabaseId> databaseIds()
    {
        return databaseIds;
    }

    private static MemberId randomMemberId()
    {
        return new MemberId( UUID.randomUUID() );
    }
}
