/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.replication.DirectReplicator;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Test;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.EpochException;

import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings( "unchecked" )
public class BarrierStateTest
{
    private final DatabaseId databaseId = new TestDatabaseIdRepository().defaultDatabase();

    @Test
    public void leaderShouldBeBarrierHolder() throws Exception
    {
        // given
        MemberId me = member( 0 );

        ReplicatedBarrierTokenStateMachine replicatedLockStateMachine =
                new ReplicatedBarrierTokenStateMachine( new InMemoryStateStorage( ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN ) );

        DirectReplicator replicator = new DirectReplicator( replicatedLockStateMachine );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( me );

        BarrierState barrierState =
                new BarrierState( me, replicator, leaderLocator, replicatedLockStateMachine, databaseId );

        // when
        barrierState.ensureHoldingToken();
    }

    @Test
    public void nonLeaderShouldNotBeBarrierHolder() throws Exception
    {
        // given
        MemberId me = member( 0 );
        MemberId leader = member( 1 );

        ReplicatedBarrierTokenStateMachine replicatedLockStateMachine =
                new ReplicatedBarrierTokenStateMachine( new InMemoryStateStorage( ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN ) );
        DirectReplicator replicator = new DirectReplicator( replicatedLockStateMachine );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( leader );

        BarrierState barrierState =
                new BarrierState( me, replicator, leaderLocator, replicatedLockStateMachine, databaseId );

        // when
        try
        {
            barrierState.ensureHoldingToken();
            fail( "Should have thrown exception" );
        }
        catch ( EpochException e )
        {
            // expected
        }
    }
}
