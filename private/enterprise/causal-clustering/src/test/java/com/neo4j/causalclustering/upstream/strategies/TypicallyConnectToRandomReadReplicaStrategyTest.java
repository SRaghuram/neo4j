/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberIds;
import static com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static com.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.fakeReadReplicaTopology;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypicallyConnectToRandomReadReplicaStrategyTest
{
    private final DatabaseId databaseId = TestDatabaseIdRepository.randomDatabaseId();
    private final MemberId myself = memberId( 0 );

    @Test
    void shouldConnectToCoreOneInTenTimesByDefault()
    {
        // given
        MemberId theCoreMemberId = memberId( 1 );
        TopologyService topologyService = new FakeTopologyService( Set.of( theCoreMemberId ), memberIds( 2, 102 ), myself, Set.of( databaseId ) );

        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), myself );

        List<MemberId> responses = new ArrayList<>();

        // when
        for ( int i = 0; i < 3; i++ )
        {
            for ( int j = 0; j < 2; j++ )
            {
                responses.add( connectionStrategy.upstreamMemberForDatabase( databaseId ).get() );
            }
            assertThat( responses, hasItem( theCoreMemberId ) );
            responses.clear();
        }

        // then
    }

    @Test
    void filtersSelf()
    {
        // given
        String groupName = "groupName";
        Config config = Config.defaults();

        TypicallyConnectToRandomReadReplicaStrategy typicallyConnectToRandomReadReplicaStrategy = new TypicallyConnectToRandomReadReplicaStrategy();
        typicallyConnectToRandomReadReplicaStrategy.inject( new TopologyServiceThatPrioritisesItself( myself, groupName ), config,
                NullLogProvider.getInstance(), myself );

        // when
        Optional<MemberId> found = typicallyConnectToRandomReadReplicaStrategy.upstreamMemberForDatabase( databaseId );

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
        MemberId otherCoreMember = memberId( 1 );
        TopologyService topologyService = new FakeTopologyService( Set.of( myself, otherCoreMember ), memberIds( 2, 4 ),
                myself, Set.of( databaseId ) );
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when
        Optional<MemberId> found = connectionStrategy.upstreamMemberForDatabase( databaseId );

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
        MemberId firstOther = memberId( 1 );
        MemberId secondOther = memberId( 2 );
        TopologyService topologyService = new FakeTopologyService(  Set.of( myself, firstOther, secondOther ),
                memberIds( 3, 5 ), myself, Set.of( databaseId ) );
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when we collect enough results to feel confident of random values
        List<MemberId> found = IntStream.range( 0, 20 )
                .mapToObj( i -> connectionStrategy.upstreamMemberForDatabase( databaseId ) )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .collect( Collectors.toList() );

        // then
        assertFalse( found.contains( myself ) );
        assertTrue( found.contains( firstOther ) );
        assertTrue( found.contains( secondOther ) );
    }
}
