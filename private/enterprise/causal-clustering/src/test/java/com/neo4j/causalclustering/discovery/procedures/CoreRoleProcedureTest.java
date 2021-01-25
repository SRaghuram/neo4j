/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.collection.Dependencies;

import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CoreRoleProcedureTest extends RoleProcedureTest
{
    private final RaftMachine raftMachine = mock( RaftMachine.class );

    CoreRoleProcedureTest()
    {
        super( CoreRoleProcedure::new );
    }

    @BeforeEach
    void addRaftMachine()
    {
        var dependencies = new Dependencies();
        dependencies.satisfyDependencies( raftMachine );
        when( databaseContext.dependencies() ).thenReturn( dependencies );
    }

    @Test
    void shouldReturnLeader() throws Exception
    {
        mockAvailableExitingDatabase();
        when( raftMachine.isLeader() ).thenReturn( true );
        assertEquals( LEADER, runProcedure() );
    }

    @Test
    void shouldReturnFollower() throws Exception
    {
        mockAvailableExitingDatabase();
        when( raftMachine.isLeader() ).thenReturn( false );
        assertEquals( FOLLOWER, runProcedure() );
    }
}
