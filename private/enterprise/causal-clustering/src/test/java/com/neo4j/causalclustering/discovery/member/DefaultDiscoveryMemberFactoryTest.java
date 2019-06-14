/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultDiscoveryMemberFactoryTest
{
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();
    private final DiscoveryMemberFactory discoveryMemberFactory = new DefaultDiscoveryMemberFactory( databaseManager );

    private final MemberId id = new MemberId( UUID.randomUUID() );

    private final DatabaseId databaseId1 = databaseIdRepository.get( "one" );
    private final DatabaseId databaseId2 = databaseIdRepository.get( "two" );
    private final DatabaseId databaseId3 = databaseIdRepository.get( "three" );

    @Test
    void shouldCreateDiscoveryMemberWithId()
    {
        var discoveryMember = discoveryMemberFactory.create( id );

        assertEquals( id, discoveryMember.id() );
    }

    @Test
    void shouldCreateDiscoveryMemberWithStartedDatabases()
    {
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId1 ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId2 ).withStoppedDatabase().register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId3 ).register();

        var discoveryMember = discoveryMemberFactory.create( id );

        assertEquals( Set.of( databaseId1, databaseId3 ), discoveryMember.startedDatabases() );
    }

    @Test
    void shouldCreateDiscoveryMemberWithNonFailedDatabases()
    {
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId1 ).withFailure( new IOException() ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId2 ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId3 ).register();

        var discoveryMember = discoveryMemberFactory.create( id );

        assertEquals( Set.of( databaseId2, databaseId3 ), discoveryMember.startedDatabases() );
    }

    @Test
    void shouldCreateDiscoveryMemberWithUnmodifiableDatabases()
    {
        var discoveryMember = discoveryMemberFactory.create( id );

        assertSame( discoveryMember.startedDatabases(), discoveryMember.startedDatabases() );
        assertThat( discoveryMember.startedDatabases(), is( empty() ) );
        assertThrows( UnsupportedOperationException.class, () -> discoveryMember.startedDatabases().add( databaseId1 ) );
    }
}
