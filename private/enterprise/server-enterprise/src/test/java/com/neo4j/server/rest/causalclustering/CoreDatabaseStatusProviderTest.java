/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static com.neo4j.server.rest.causalclustering.ClusteringDatabaseStatusUtil.coreStatusMockBuilder;
import static java.time.Duration.ofMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoreDatabaseStatusProviderTest
{
    private final RaftMemberId memberId = IdFactory.randomRaftMemberId();
    private final RaftMemberId leaderId = IdFactory.randomRaftMemberId();

    private final GraphDatabaseAPI db = coreStatusMockBuilder()
            .memberId( memberId )
            .leaderId( leaderId )
            .databaseId( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) )
            .healthy( true )
            .durationSinceLastMessage( ofMillis( 42 ) )
            .appliedCommandIndex( 42 )
            .throughput( 42 )
            .role( LEADER )
            .build();

    @Test
    void shouldReturnRole()
    {
        var provider = new CoreDatabaseStatusProvider( db );

        assertEquals( LEADER, provider.currentRole() );
    }

    @Test
    void shouldReturnStatus()
    {
        var provider = new CoreDatabaseStatusProvider( db );

        var status = provider.currentStatus();

        assertEquals( 42L, status.getLastAppliedRaftIndex() );
        assertTrue( status.isParticipatingInRaftGroup() );
        assertThat( status.getVotingMembers(), containsInAnyOrder( memberId.uuid().toString(), leaderId.uuid().toString() ) );
        assertTrue( status.isHealthy() );
        assertEquals( memberId.uuid().toString(), status.getMemberId() );
        assertEquals( leaderId.uuid().toString(), status.getLeader() );
        assertEquals( 42L, status.getMillisSinceLastLeaderMessage() );
        assertEquals( 42.0, status.getRaftCommandsPerSecond() );
        assertTrue( status.isCore() );
    }
}
