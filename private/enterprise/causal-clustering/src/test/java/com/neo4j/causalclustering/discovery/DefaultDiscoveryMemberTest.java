/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultDiscoveryMemberTest
{
    DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @Test
    void shouldReturnMemberId()
    {
        MemberId memberId = new MemberId( UUID.randomUUID() );

        DiscoveryMember discoveryMember = new DefaultDiscoveryMember( memberId, new StubClusteredDatabaseManager() );

        assertEquals( memberId, discoveryMember.id() );
    }

    @Test
    void shouldReturnHostedDatabases()
    {
        DatabaseId databaseId1 = databaseIdRepository.get( "one" );
        DatabaseId databaseId2 = databaseIdRepository.get( "two" );
        DatabaseId databaseId3 = databaseIdRepository.get( "three" );

        StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId1 ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId2 ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId3 ).register();

        DiscoveryMember discoveryMember = new DefaultDiscoveryMember( new MemberId( UUID.randomUUID() ), databaseManager );

        assertEquals( Set.of( databaseId1, databaseId2, databaseId3 ), discoveryMember.hostedDatabases() );
    }

    @Test
    void shouldReturnUnmodifiableHostedDatabases()
    {
        StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();

        DiscoveryMember discoveryMember = new DefaultDiscoveryMember( new MemberId( UUID.randomUUID() ), databaseManager );

        Set<DatabaseId> databaseIds = discoveryMember.hostedDatabases();
        assertEquals( Set.of(), databaseIds );
        assertThrows( UnsupportedOperationException.class, () -> databaseIds.add( databaseIdRepository.get( "one" ) ) );
    }
}
