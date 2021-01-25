/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.kernel.database.NamedDatabaseId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class DefaultLeaderServiceTest
{
    private final NamedDatabaseId dbone = from( "one", UUID.randomUUID() );
    private final NamedDatabaseId dbtwo = from( "one", UUID.randomUUID() );

    private final CoreServerIdentity coreA = new InMemoryCoreServerIdentity();
    private final CoreServerIdentity coreB = new InMemoryCoreServerIdentity();

    private final CoreServerIdentity topologyLeader = new InMemoryCoreServerIdentity();

    private TopologyService topologyService = mock( TopologyService.class );
    private DefaultLeaderService leaderService = new DefaultLeaderService( topologyService, nullLogProvider() );
    private LeaderListener dbOneListener;
    private LeaderListener dbTwoListener;

    @BeforeEach
    void shouldGiveCorrectLeader()
    {
        dbOneListener = leaderService.createListener( dbone );
        dbTwoListener = leaderService.createListener( dbtwo );
        when( topologyService.getLeader( any( NamedDatabaseId.class ) ) ).thenReturn( leaderInfo( topologyLeader.raftMemberId( dbone ) ) );
    }

    @Test
    void shouldProvideCorrectLeader()
    {
        when( topologyService.resolveServerForRaftMember( coreA.raftMemberId( dbone ) ) ).thenReturn( coreA.serverId() );
        when( topologyService.resolveServerForRaftMember( coreB.raftMemberId( dbtwo ) ) ).thenReturn( coreB.serverId() );

        dbOneListener.onLeaderSwitch( leaderInfo( coreA.raftMemberId( dbone ) ) );
        dbTwoListener.onLeaderSwitch( leaderInfo( coreB.raftMemberId( dbtwo ) ) );

        assertThat( leaderService.getLeaderId( dbone ) ).contains( coreA.serverId() );
        assertThat( leaderService.getLeaderId( dbtwo ) ).contains( coreB.serverId() );
    }

    @Test
    void shouldReactToLeaderSwitch()
    {
        when( topologyService.resolveServerForRaftMember( coreA.raftMemberId( dbone ) ) ).thenReturn( coreA.serverId() );
        when( topologyService.resolveServerForRaftMember( coreB.raftMemberId( dbone ) ) ).thenReturn( coreB.serverId() );

        dbOneListener.onLeaderSwitch( leaderInfo( coreA.raftMemberId( dbone ) ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( coreA.serverId() );

        dbOneListener.onLeaderSwitch( leaderInfo( coreB.raftMemberId( dbone ) ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( coreB.serverId() );
    }

    @Test
    void shouldFallbackToDiscovery()
    {
        when( topologyService.resolveServerForRaftMember( topologyLeader.raftMemberId( dbone ) ) ).thenReturn( topologyLeader.serverId() );
        when( topologyService.resolveServerForRaftMember( coreA.raftMemberId( dbone ) ) ).thenReturn( coreA.serverId() );

        dbOneListener.onLeaderSwitch( leaderInfo( coreA.raftMemberId( dbone ) ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( coreA.serverId() );

        dbOneListener.onLeaderSwitch( leaderInfo( null ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( topologyLeader.serverId() );
    }

    @Test
    void shoulReactToUnregisterEvent()
    {
        when( topologyService.resolveServerForRaftMember( topologyLeader.raftMemberId( dbone ) ) ).thenReturn( topologyLeader.serverId() );
        when( topologyService.resolveServerForRaftMember( coreA.raftMemberId( dbone ) ) ).thenReturn( coreA.serverId() );

        dbOneListener.onLeaderSwitch( leaderInfo( coreA.raftMemberId( dbone ) ) );
        assertThat( leaderService.getLeaderId( dbone ) ).contains( coreA.serverId() );

        dbOneListener.onUnregister();
        assertThat( leaderService.getLeaderId( dbone ) ).contains( topologyLeader.serverId() );
    }

    private static LeaderInfo leaderInfo( RaftMemberId memberOne )
    {
        return new LeaderInfo( memberOne, 1 );
    }
}
