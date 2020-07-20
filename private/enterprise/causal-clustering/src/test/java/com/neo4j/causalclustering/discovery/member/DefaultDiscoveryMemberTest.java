/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultDiscoveryMemberTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    private final DatabaseId databaseId1 = databaseIdRepository.getRaw( "one" ).databaseId();
    private final DatabaseId databaseId2 = databaseIdRepository.getRaw( "two" ).databaseId();

    @Test
    void shouldReturnMemberId()
    {
        var id = new ServerId( UUID.randomUUID() );

        var discoveryMember = new DefaultDiscoveryMember( id, Set.of( databaseId1, databaseId2 ) );

        assertEquals( id, discoveryMember.id() );
    }

    @Test
    void shouldReturnStartedDatabases()
    {
        var id = new ServerId( UUID.randomUUID() );
        var startedDatabases = Set.of( databaseId1, databaseId2 );

        var discoveryMember = new DefaultDiscoveryMember( id, startedDatabases );

        assertEquals( startedDatabases, discoveryMember.startedDatabases() );
    }
}
