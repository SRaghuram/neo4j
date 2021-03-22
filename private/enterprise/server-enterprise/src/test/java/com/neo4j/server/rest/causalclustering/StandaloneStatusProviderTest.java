/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.server.rest.causalclustering.ClusteringDatabaseStatusUtil.standaloneStatusMockBuilder;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class StandaloneStatusProviderTest
{
    private final ServerId myself = new ServerId( randomUUID() );

    private final NamedDatabaseId databaseId = randomNamedDatabaseId();

    private final GraphDatabaseAPI db = standaloneStatusMockBuilder()
            .databaseId( databaseId )
            .healthy( true )
            .myself( myself )
            .build();

    @Test
    void shouldReturnStatus()
    {
        var provider = new StandaloneDatabaseStatusProvider( db );

        var status = provider.currentStatus();

        assertEquals( 0L, status.getLastAppliedRaftIndex() );
        assertFalse( status.isParticipatingInRaftGroup() );
        assertThat( status.getVotingMembers(), empty() );
        assertTrue( status.isHealthy() );
        assertEquals( myself.uuid().toString(), status.getMemberId() );
        assertEquals( myself.uuid().toString(), status.getLeader() );
        assertNull( status.getMillisSinceLastLeaderMessage() );
        assertNull( status.getRaftCommandsPerSecond() );
        assertFalse( status.isCore() );
    }
}
