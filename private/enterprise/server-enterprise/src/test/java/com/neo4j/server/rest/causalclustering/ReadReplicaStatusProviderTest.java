/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.server.rest.causalclustering.ClusteringDatabaseStatusUtil.readReplicaStatusMockBuilder;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class ReadReplicaStatusProviderTest
{
    private final MemberId coreId1 = MemberId.of( randomUUID() );
    private final MemberId coreId2 = MemberId.of( randomUUID() );
    private final MemberId readReplicaId = MemberId.of( randomUUID() );

    private final NamedDatabaseId databaseId = randomNamedDatabaseId();

    private final GraphDatabaseAPI db = readReplicaStatusMockBuilder()
            .databaseId( databaseId )
            .healthy( true )
            .readReplicaId( readReplicaId )
            .coreRole( coreId1, FOLLOWER )
            .coreRole( coreId2, LEADER )
            .appliedCommandIndex( 42 )
            .throughput( 42 )
            .build();

    @Test
    void shouldReturnStatus()
    {
        var provider = new ReadReplicaDatabaseStatusProvider( db );

        var status = provider.currentStatus();

        assertEquals( 42L, status.getLastAppliedRaftIndex() );
        assertFalse( status.isParticipatingInRaftGroup() );
        assertThat( status.getVotingMembers(), containsInAnyOrder( coreId1.getUuid().toString(), coreId2.getUuid().toString() ) );
        assertTrue( status.isHealthy() );
        assertEquals( readReplicaId.getUuid().toString(), status.getMemberId() );
        assertEquals( coreId2.getUuid().toString(), status.getLeader() );
        assertNull( status.getMillisSinceLastLeaderMessage() );
        assertEquals( 42.0, status.getRaftCommandsPerSecond() );
        assertFalse( status.isCore() );
    }
}
