/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.server.rest.causalclustering.ClusteringDatabaseStatusUtil.readReplicaStatusMockBuilder;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class ReadReplicaStatusProviderTest
{
    private final ServerId coreId1 = new ServerId( randomUUID() );
    private final ServerId coreId2 = new ServerId( randomUUID() );
    private final ServerId readReplicaId = new ServerId( randomUUID() );

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
        assertThat( status.getVotingMembers(), empty() );// containsInAnyOrder( coreId1.uuid().toString(), coreId2.uuid().toString() ) );
        assertTrue( status.isHealthy() );
        assertEquals( readReplicaId.uuid().toString(), status.getMemberId() );
        assertEquals( coreId2.uuid().toString(), status.getLeader() );
        assertNull( status.getMillisSinceLastLeaderMessage() );
        assertEquals( 42.0, status.getRaftCommandsPerSecond() );
        assertFalse( status.isCore() );
    }
}
