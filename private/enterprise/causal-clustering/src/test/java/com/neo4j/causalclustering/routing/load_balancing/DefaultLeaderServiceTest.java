/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.NullLogProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

class DefaultLeaderServiceTest
{
    private final NamedDatabaseId dbone = from( "one", UUID.randomUUID() );
    private final NamedDatabaseId dbtwo = from( "one", UUID.randomUUID() );

    private final RaftMemberId memberOne = IdFactory.randomRaftMemberId();
    private final RaftMemberId memberTwo = IdFactory.randomRaftMemberId();
    private final RaftMemberId topologyLeader = IdFactory.randomRaftMemberId();

    private TopologyService topologyService = mock( TopologyService.class );
    private DefaultLeaderService leaderService = new DefaultLeaderService( topologyService, NullLogProvider.nullLogProvider() );
    private LeaderListener dbOneListener;
    private LeaderListener dbTwoListener;

    @BeforeEach
    void shouldGiveCorrectLeader()
    {
        dbOneListener = leaderService.createListener( dbone );
        dbTwoListener = leaderService.createListener( dbtwo );
        when( topologyService.getLeader( any( NamedDatabaseId.class ) ) ).thenReturn( leaderInfo( topologyLeader ) );
    }

    @Test
    void shouldProvideCorrectLeader()
    {
        dbOneListener.onLeaderSwitch( leaderInfo( memberOne ) );
        dbTwoListener.onLeaderSwitch( leaderInfo( memberTwo ) );

        assertThat( leaderService.getLeaderId( dbone ) ).contains( memberOne.serverId() );
        assertThat( leaderService.getLeaderId( dbtwo ) ).contains( memberTwo.serverId() );
    }

    @Test
    void shouldReactToLeaderSwitch()
    {
        dbOneListener.onLeaderSwitch( leaderInfo( memberOne ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( memberOne.serverId() );

        dbOneListener.onLeaderSwitch( leaderInfo( memberTwo ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( memberTwo.serverId() );
    }

    @Test
    void shouldFallbackToDiscovery()
    {
        dbOneListener.onLeaderSwitch( leaderInfo( memberOne ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( memberOne.serverId() );

        dbOneListener.onLeaderSwitch( leaderInfo( null ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( topologyLeader.serverId() );
    }

    @Test
    void shoulReactToUnregisterEvent()
    {
        dbOneListener.onLeaderSwitch( leaderInfo( memberOne ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( memberOne.serverId() );
        dbOneListener.onUnregister();
        assertThat( leaderService.getLeaderId( dbone ) ).contains( topologyLeader.serverId() );
    }

    private static LeaderInfo leaderInfo( RaftMemberId memberOne )
    {
        return new LeaderInfo( memberOne, 1 );
    }
}
