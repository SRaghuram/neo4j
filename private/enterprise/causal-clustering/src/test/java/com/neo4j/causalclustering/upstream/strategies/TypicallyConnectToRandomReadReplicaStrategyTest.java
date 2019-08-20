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
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

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
    private final MemberId myself = new MemberId( new UUID( 1234, 5678 ) );

    @Test
    void shouldConnectToCoreOneInTenTimesByDefault()
    {
        // given
        MemberId theCoreMemberId = new MemberId( UUID.randomUUID() );
        TopologyService topologyService = new FakeTopologyService( fakeCoreTopology( theCoreMemberId ),
                fakeReadReplicaTopology( UserDefinedConfigurationStrategyTest.memberIDs( 100 ) ) );

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
        MemberId otherCoreMember = new MemberId( new UUID( 12, 34 ) );
        TopologyService topologyService = new FakeTopologyService( fakeCoreTopology( myself, otherCoreMember ),
                fakeReadReplicaTopology( UserDefinedConfigurationStrategyTest.memberIDs( 2 ) ) );
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
        MemberId firstOther = new MemberId( new UUID( 12, 34 ) );
        MemberId secondOther = new MemberId( new UUID( 56, 78 ) );
        TopologyService topologyService = new FakeTopologyService( fakeCoreTopology( myself, firstOther, secondOther ),
                fakeReadReplicaTopology( UserDefinedConfigurationStrategyTest.memberIDs( 2 ) ) );
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
