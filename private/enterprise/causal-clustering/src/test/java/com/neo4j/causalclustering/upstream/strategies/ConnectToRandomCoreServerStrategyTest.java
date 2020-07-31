/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConnectToRandomCoreServerStrategyTest
{
    private static final NamedDatabaseId NAMED_DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();

    @Test
    void shouldConnectToRandomCoreServer() throws Exception
    {
        // given
        MemberId memberId1 = new MemberId( UUID.randomUUID() );
        MemberId memberId2 = new MemberId( UUID.randomUUID() );
        MemberId memberId3 = new MemberId( UUID.randomUUID() );

        TopologyService topologyService = mock( TopologyService.class );
        when( topologyService.coreTopologyForDatabase( NAMED_DATABASE_ID ) ).thenReturn( fakeCoreTopology( memberId1, memberId2, memberId3 ) );

        ConnectToRandomCoreServerStrategy connectionStrategy = new ConnectToRandomCoreServerStrategy();
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), null );

        // when
        Optional<MemberId> memberId = connectionStrategy.upstreamMemberForDatabase( NAMED_DATABASE_ID );

        // then
        assertTrue( memberId.isPresent() );
        assertThat( memberId.get(), anyOf( equalTo( memberId1 ), equalTo( memberId2 ), equalTo( memberId3 ) ) );
    }

    @Test
    void filtersSelf() throws UpstreamDatabaseSelectionException
    {
        // given
        MemberId myself = new MemberId( new UUID( 1234, 5678 ) );
        Config config = Config.defaults();
        var groupName = new ServerGroupName( "groupName" );

        // and
        ConnectToRandomCoreServerStrategy connectToRandomCoreServerStrategy = new ConnectToRandomCoreServerStrategy();
        connectToRandomCoreServerStrategy.inject( new TopologyServiceThatPrioritisesItself( myself, groupName ), config, NullLogProvider.getInstance(),
                myself );

        // when
        Optional<MemberId> found = connectToRandomCoreServerStrategy.upstreamMemberForDatabase( NAMED_DATABASE_ID );

        // then
        assertTrue( found.isPresent() );
        assertNotEquals( myself, found );
    }

    static DatabaseCoreTopology fakeCoreTopology( MemberId... memberIds )
    {
        assertThat( memberIds, arrayWithSize( greaterThan( 0 ) ) );

        RaftId raftId = RaftIdFactory.random();
        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : memberIds )
        {
            coreMembers.put( memberId, TestTopology.addressesForCore( offset, false ) );
            offset++;
        }

        return new DatabaseCoreTopology( NAMED_DATABASE_ID.databaseId(), raftId, coreMembers );
    }
}
