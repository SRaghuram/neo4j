/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.OnDemandJobScheduler;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MembershipWaiterTest
{
    private DatabaseHealth dbHealth = mock( DatabaseHealth.class );

    @Before
    public void mocking()
    {
        when( dbHealth.isHealthy() ).thenReturn( true );
    }

    @Test
    public void shouldReturnImmediatelyIfMemberAndCaughtUp() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter waiter = new MembershipWaiter( member( 0 ), jobScheduler, () -> dbHealth, 500,
                NullLogProvider.getInstance(), new Monitors() );

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        ExposedRaftState raftState = RaftStateBuilder.raftState()
                .votingMembers( member( 0 ) )
                .leaderCommit( 0 )
                .entryLog( raftLog )
                .commitIndex( 0L )
                .build().copy();

        RaftMachine raft = mock( RaftMachine.class );
        when( raft.state() ).thenReturn( raftState );

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raft );
        jobScheduler.runJob();
        jobScheduler.runJob();

        future.get( 0, NANOSECONDS );
    }

    @Test
    public void shouldWaitUntilLeaderCommitIsAvailable() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter waiter = new MembershipWaiter( member( 0 ), jobScheduler, () -> dbHealth, 500,
                NullLogProvider.getInstance(), new Monitors() );

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        ExposedRaftState raftState = RaftStateBuilder.raftState()
                .votingMembers( member( 0 ) )
                .leaderCommit( 0 )
                .entryLog( raftLog )
                .commitIndex( 0L )
                .build().copy();

        RaftMachine raft = mock( RaftMachine.class );
        when( raft.state() ).thenReturn( raftState );

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raft );
        jobScheduler.runJob();

        future.get( 1, TimeUnit.SECONDS );
    }

    @Test
    public void shouldTimeoutIfCaughtUpButNotMember() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter waiter = new MembershipWaiter( member( 0 ), jobScheduler, () -> dbHealth, 1,
                NullLogProvider.getInstance(), new Monitors() );

        ExposedRaftState raftState = RaftStateBuilder.raftState()
                .votingMembers( member( 1 ) )
                .leaderCommit( 0 )
                .build().copy();

        RaftMachine raft = mock( RaftMachine.class );
        when( raft.state() ).thenReturn( raftState );

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raft );
        jobScheduler.runJob();
        jobScheduler.runJob();

        try
        {
            future.get( 10, MILLISECONDS );
            fail( "Should have timed out." );
        }
        catch ( TimeoutException e )
        {
            // expected
        }
    }

    @Test
    public void shouldTimeoutIfMemberButNotCaughtUp() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter waiter = new MembershipWaiter( member( 0 ), jobScheduler, () -> dbHealth, 1,
                NullLogProvider.getInstance(), new Monitors() );

        ExposedRaftState raftState = RaftStateBuilder.raftState()
                .votingMembers( member( 0 ), member( 1 ) )
                .leaderCommit( 0 )
                .build().copy();

        RaftMachine raft = mock( RaftMachine.class );
        when( raft.state() ).thenReturn( raftState );

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raft );
        jobScheduler.runJob();
        jobScheduler.runJob();

        try
        {
            future.get( 10, MILLISECONDS );
            fail( "Should have timed out." );
        }
        catch ( TimeoutException e )
        {
            // expected
        }
    }

    @Test
    public void shouldTimeoutIfLeaderCommitIsNeverKnown() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter waiter = new MembershipWaiter( member( 0 ), jobScheduler, () -> dbHealth, 1,
                NullLogProvider.getInstance(), new Monitors() );

        ExposedRaftState raftState = RaftStateBuilder.raftState()
                .leaderCommit( -1 )
                .build().copy();

        RaftMachine raft = mock( RaftMachine.class );
        when(raft.state()).thenReturn( raftState );

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raft );
        jobScheduler.runJob();

        try
        {
            future.get( 10, MILLISECONDS );
            fail( "Should have timed out." );
        }
        catch ( TimeoutException e )
        {
            // expected
        }
    }
}
