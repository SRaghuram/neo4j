/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;
import java.util.UUID;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TestDiscoveryMember implements DiscoveryMember
{
    private final MemberId id;
    private final Set<String> databaseNames;

    public TestDiscoveryMember()
    {
        this( new MemberId( UUID.randomUUID() ) );
    }

    public TestDiscoveryMember( MemberId memberId )
    {
        this( memberId, Set.of( DEFAULT_DATABASE_NAME ) );
    }

    public TestDiscoveryMember( MemberId id, Set<String> databaseNames )
    {
        this.id = id;
        this.databaseNames = databaseNames;
    }

    @Override
    public MemberId id()
    {
        return id;
    }

    @Override
    public Set<String> databaseNames()
    {
        return databaseNames;
    }
}
