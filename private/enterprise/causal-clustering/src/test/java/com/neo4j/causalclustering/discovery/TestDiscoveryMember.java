/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.member.DiscoveryMember;
import com.neo4j.causalclustering.identity.IdFactory;

import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

public class TestDiscoveryMember implements DiscoveryMember
{
    private final ServerId id;
    private final Set<NamedDatabaseId> startedDatabases;

    public TestDiscoveryMember()
    {
        this( IdFactory.randomServerId() );
    }

    public TestDiscoveryMember( ServerId memberId )
    {
        this( memberId, Set.of( new TestDatabaseIdRepository().defaultDatabase() ) );
    }

    public TestDiscoveryMember( ServerId id, Set<NamedDatabaseId> startedDatabases )
    {
        this.id = id;
        this.startedDatabases = startedDatabases;
    }

    @Override
    public ServerId id()
    {
        return id;
    }

    @Override
    public Set<DatabaseId> startedDatabases()
    {
        return startedDatabases.stream().map( NamedDatabaseId::databaseId ).collect( Collectors.toSet() );
    }
}
