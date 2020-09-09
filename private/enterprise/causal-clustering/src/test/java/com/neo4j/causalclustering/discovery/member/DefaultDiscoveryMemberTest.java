/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.Test;

import java.util.Set;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultDiscoveryMemberTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    private final NamedDatabaseId databaseId1 = databaseIdRepository.getRaw( "one" );
    private final NamedDatabaseId databaseId2 = databaseIdRepository.getRaw( "two" );

    @Test
    void shouldReturnMemberId()
    {
        var id = IdFactory.randomServerId();

        var discoveryMember = new DefaultDiscoveryMember( id, Set.of( databaseId1, databaseId2 ) );

        assertEquals( id, discoveryMember.id() );
    }

    @Test
    void shouldReturnStartedDatabases()
    {
        var id = IdFactory.randomServerId();

        var startedDatabases = Set.of( databaseId1, databaseId2 );

        var discoveryMember = new DefaultDiscoveryMember( id, startedDatabases );

        assertEquals( startedDatabases, discoveryMember.startedDatabases() );
    }
}
