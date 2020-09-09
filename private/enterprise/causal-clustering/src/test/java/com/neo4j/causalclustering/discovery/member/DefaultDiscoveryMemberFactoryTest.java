/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultDiscoveryMemberFactoryTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();
    private final DatabaseStateService databaseStateService = mock( DatabaseStateService.class );
    private final DiscoveryMemberFactory discoveryMemberFactory = new DefaultDiscoveryMemberFactory( databaseManager, databaseStateService );

    private final ServerId id = IdFactory.randomServerId();

    private final NamedDatabaseId namedDatabaseId1 = databaseIdRepository.getRaw( "one" );
    private final NamedDatabaseId namedDatabaseId2 = databaseIdRepository.getRaw( "two" );
    private final NamedDatabaseId namedDatabaseId3 = databaseIdRepository.getRaw( "three" );

    @Test
    void shouldCreateDiscoveryMemberWithId()
    {
        var discoveryMember = discoveryMemberFactory.create( id );

        assertEquals( id, discoveryMember.id() );
    }

    @Test
    void shouldCreateDiscoveryMemberWithStartedDatabases()
    {
        databaseManager.givenDatabaseWithConfig().withDatabaseId( namedDatabaseId1 ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( namedDatabaseId2 ).withStoppedDatabase().register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( namedDatabaseId3 ).register();

        var discoveryMember = discoveryMemberFactory.create( id );

        assertEquals( Set.of( namedDatabaseId1, namedDatabaseId3 ), discoveryMember.startedDatabases() );
    }

    @Test
    void shouldCreateDiscoveryMemberWithNonFailedDatabases()
    {
        when( databaseStateService.causeOfFailure( namedDatabaseId1 ) ).thenReturn( Optional.of( new IOException() ) );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( namedDatabaseId1 ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( namedDatabaseId2 ).register();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( namedDatabaseId3 ).register();

        var discoveryMember = discoveryMemberFactory.create( id );

        assertEquals( Set.of( namedDatabaseId2, namedDatabaseId3 ), discoveryMember.startedDatabases() );
    }

    @Test
    void shouldCreateDiscoveryMemberWithUnmodifiableDatabases()
    {
        var discoveryMember = discoveryMemberFactory.create( id );

        assertSame( discoveryMember.startedDatabases(), discoveryMember.startedDatabases() );
        assertThat( discoveryMember.startedDatabases(), is( empty() ) );
        assertThrows( UnsupportedOperationException.class, () -> discoveryMember.startedDatabases().add( namedDatabaseId1 ) );
    }
}
