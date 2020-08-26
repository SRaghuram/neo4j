/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeaderProviderTest
{
    private NamedDatabaseId databaseId = DatabaseIdFactory.from( "system", UUID.randomUUID() );
    private RaftMemberId leaderFromRaft = IdFactory.randomRaftMemberId();
    private RaftMemberId leaderFromDiscovery = IdFactory.randomRaftMemberId();

    @Test
    void shouldReturnEmptyIfBothAreEmpty()
    {
        // when Raft and Discovery both deliver empty
        assertNull( setup( 0, 0 ).getLeader( databaseId ) );
    }

    @Test
    void shouldReturnNotEmpty()
    {
        // when Raft is empty and Discovery is not
        var resultForEmptyRaft = setup( 0, 1 ).getLeader( databaseId );
        assertEquals( resultForEmptyRaft, leaderFromDiscovery );

        // when Discovery is empty and Raft is not
        var resultForEmptyDiscovery = setup( 1, 0 ).getLeader( databaseId );
        assertEquals( resultForEmptyDiscovery, leaderFromRaft );
    }

    @Test
    void shouldReturnGreater()
    {
        // when Raft and Discovery both deliver and Discovery is older
        var resultForRaftNewer = setup( 2, 1 ).getLeader( databaseId );
        assertEquals( resultForRaftNewer, leaderFromRaft );

        // when Raft and Discovery both deliver and Discovery is newer
        var resultForDiscoveryNewer = setup( 1, 2 ).getLeader( databaseId );
        assertEquals( resultForDiscoveryNewer, leaderFromDiscovery );
    }

    @Test
    public void shouldReturnRaftIfEquals()
    {
        // when Raft and Discovery both deliver the same term
        var result = setup( 1, 1 ).getLeader( databaseId );
        assertEquals( result, leaderFromRaft );
    }

    @Test
    void shouldReturnEmptyIfDiscoveryIsHalfFilled()
    {
        // when Raft is empty and Discovery delivers LeaderInfo without memberId
        assertNull( setup( 0, -1 ).getLeader( databaseId ) );
    }

    @Test
    void shouldReturnRaftIfDiscoveryIsHalfFilled()
    {
        // when Raft delivers and Discovery delivers LeaderInfo without memberId
        var result = setup( 1, -1 ).getLeader( databaseId );
        assertEquals( result, leaderFromRaft );
    }

    private LeaderProvider setup( int termFromRaft, int termFromDiscovery )
    {
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        TopologyService topologyService = mock( TopologyService.class );

        if ( termFromRaft == 0 )
        {
            when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.empty() );
        }
        else
        {
            LeaderInfo leaderFromRaft = new LeaderInfo( this.leaderFromRaft, termFromRaft );
            when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.of( leaderFromRaft ) );
        }

        if ( termFromDiscovery == 0 )
        {
            when( topologyService.getLeader( databaseId ) ).thenReturn( null );
        }
        else if ( termFromDiscovery < 0 )
        {
            LeaderInfo leaderFromDiscovery = new LeaderInfo( null, -termFromDiscovery );
            when( topologyService.getLeader( databaseId ) ).thenReturn( leaderFromDiscovery );
        }
        else
        {
            LeaderInfo leaderFromDiscovery = new LeaderInfo( this.leaderFromDiscovery, termFromDiscovery );
            when( topologyService.getLeader( databaseId ) ).thenReturn( leaderFromDiscovery );
        }
        return new LeaderProvider( leaderLocator, topologyService );
    }
}
