/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverIds;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypicallyConnectToRandomReadReplicaStrategyTest
{
    private final NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
    private final ServerId myself = serverId( 0 );

    @Test
    void shouldConnectToCoreOneInTenTimesByDefault()
    {
        // given
        ServerId theCoreServerId = serverId( 1 );
        var topologyService = new FakeTopologyService( Set.of( theCoreServerId ), serverIds( 2, 102 ),
                myself, Set.of( namedDatabaseId ) );

        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), myself );

        List<ServerId> responses = new ArrayList<>();

        // when
        for ( int i = 0; i < 3; i++ )
        {
            for ( int j = 0; j < 2; j++ )
            {
                responses.add( connectionStrategy.upstreamServerForDatabase( namedDatabaseId ).get() );
            }
            assertThat( responses, hasItem( theCoreServerId ) );
            responses.clear();
        }

        // then
    }

    @Test
    void filtersSelf()
    {
        // given
        var groupName = new ServerGroupName( "groupName" );
        Config config = Config.defaults();

        var typicallyConnectToRandomReadReplicaStrategy = new TypicallyConnectToRandomReadReplicaStrategy();
        typicallyConnectToRandomReadReplicaStrategy.inject( new TopologyServiceThatPrioritisesItself( myself, groupName ), config,
                NullLogProvider.getInstance(), myself );

        // when
        Optional<ServerId> found = typicallyConnectToRandomReadReplicaStrategy.upstreamServerForDatabase( namedDatabaseId );

        // then
        assertTrue( found.isPresent() );
        assertNotEquals( myself, found );
    }

    @Test
    void onCounterTriggerFiltersSelf()
    {
        // given counter always triggers to get a core member
        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 1 );

        // and requesting core member will return self and another member
        ServerId otherCoreServer = serverId( 1 );
        var topologyService = new FakeTopologyService( Set.of( myself, otherCoreServer ), serverIds( 2, 4 ),
                myself, Set.of( namedDatabaseId ) );
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when
        Optional<ServerId> found = connectionStrategy.upstreamServerForDatabase( namedDatabaseId );

        // then
        assertTrue( found.isPresent() );
        assertNotEquals( myself, found.get() );
    }

    @Test
    void randomCoreDoesNotReturnSameCoreTwice()
    {
        // given counter always core member
        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 1 );

        // and
        ServerId firstOther = serverId( 1 );
        ServerId secondOther = serverId( 2 );
        var topologyService = new FakeTopologyService( Set.of( myself, firstOther, secondOther ),
                serverIds( 3, 5 ), myself, Set.of( namedDatabaseId ) );
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when we collect enough results to feel confident of random values
        List<ServerId> found = IntStream.range( 0, 20 )
                .mapToObj( i -> connectionStrategy.upstreamServerForDatabase( namedDatabaseId ) )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .collect( Collectors.toList() );

        // then
        assertFalse( found.contains( myself ) );
        assertTrue( found.contains( firstOther ) );
        assertTrue( found.contains( secondOther ) );
    }
}
