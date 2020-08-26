/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.leader_transfer.ExpiringSet;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import com.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.ACTIVE_ELECTION;
import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder.builder;
import static com.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static com.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftStateTest
{

    @Test
    void shouldUpdateCacheState() throws Exception
    {
        //Test that updates applied to the raft state will be reflected in the entry cache.

        //given
        InFlightCache cache = new ConsecutiveInFlightCache();
        RaftState raftState = new RaftState( raftMember( 0 ),
                new InMemoryStateStorage<>( new TermState() ), new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState() ), cache, NullLogProvider.getInstance(),
                new ExpiringSet<>( Duration.ofSeconds( 1 ), new FakeClock() ) );

        List<RaftLogCommand> logCommands = new LinkedList<>()
        {{
            add( new AppendLogEntry( 1, new RaftLogEntry( 0L, valueOf( 0 ) ) ) );
            add( new AppendLogEntry( 2, new RaftLogEntry( 0L, valueOf( 1 ) ) ) );
            add( new AppendLogEntry( 3, new RaftLogEntry( 0L, valueOf( 2 ) ) ) );
            add( new AppendLogEntry( 4, new RaftLogEntry( 0L, valueOf( 4 ) ) ) );
            add( new TruncateLogCommand( 3 ) );
            add( new AppendLogEntry( 3, new RaftLogEntry( 0L, valueOf( 5 ) ) ) );
        }};

        OutcomeBuilder raftTestMemberOutcome = builder().setRole( CANDIDATE ).renewElectionTimer( ACTIVE_ELECTION );

        for ( RaftLogCommand logCommand : logCommands )
        {
            raftTestMemberOutcome.addLogCommand( logCommand );
        }

        //when
        raftState.update( raftTestMemberOutcome.build() );

        //then
        Assertions.assertNotNull( cache.get( 1L ) );
        Assertions.assertNotNull( cache.get( 2L ) );
        Assertions.assertNotNull( cache.get( 3L ) );
        Assertions.assertEquals( valueOf( 5 ), cache.get( 3L ).content() );
        Assertions.assertNull( cache.get( 4L ) );
    }

    @Test
    void shouldRemoveFollowerStateAfterBecomingLeader() throws Exception
    {
        // given
        RaftState raftState = new RaftState( raftMember( 0 ),
                new InMemoryStateStorage<>( new TermState() ),
                new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState() ),
                new ConsecutiveInFlightCache(), NullLogProvider.getInstance(),
                new ExpiringSet<>( Duration.ofSeconds( 1 ), new FakeClock() ) );

        raftState.update( builder().setRole( CANDIDATE ).replaceFollowerStates( initialFollowerStates() ).renewElectionTimer( ACTIVE_ELECTION ).build() );

        // when
        raftState.update( builder().setRole( CANDIDATE ).replaceFollowerStates( new FollowerStates<>() ).renewElectionTimer( ACTIVE_ELECTION ).build() );

        // then
        Assertions.assertEquals( 0, raftState.followerStates().size() );
    }

    @Test
    void shouldCorrectlyTimeoutLeadershipTransfers()
    {
        // given
        var clock = new FakeClock();
        var leadershipTransfers = new ExpiringSet<RaftMemberId>( Duration.ofSeconds( 2 ), clock );

        var raftState = new RaftState( raftMember( 0 ),
                new InMemoryStateStorage<>( new TermState() ),
                new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState() ),
                new ConsecutiveInFlightCache(), NullLogProvider.getInstance(),
                leadershipTransfers );

        // when
        leadershipTransfers.add( raftMember( 1 ) );
        clock.forward( Duration.ofSeconds( 1 ) );

        // then
        assertTrue( raftState.areTransferringLeadership() );

        // when
        clock.forward( Duration.ofSeconds( 2 ) );
        assertFalse( raftState.areTransferringLeadership() );
    }

    @Test
    void shouldActivateDeactivateLeadershipTransferTimer() throws IOException
    {
        // given
        var timer = new ExpiringSet<RaftMemberId>( Duration.ofSeconds( 2 ), new FakeClock() );

        var raftState = new RaftState( raftMember( 0 ),
                new InMemoryStateStorage<>( new TermState() ),
                new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState() ),
                new ConsecutiveInFlightCache(), NullLogProvider.getInstance(), timer );

        var outcomeA = OutcomeBuilder.builder( LEADER, raftState );
        outcomeA.startTransferringLeadership( raftMember( 1 ) );

        assertFalse( raftState.areTransferringLeadership() );

        // when
        raftState.update( outcomeA.build() );

        // then
        assertTrue( raftState.areTransferringLeadership() );

        // when
        var outcomeB = OutcomeBuilder.builder( LEADER, raftState );
        var rejection = new RaftMessages.LeadershipTransfer.Rejection( raftMember( 1 ), 2, 1 );
        outcomeB.addLeaderTransferRejection( rejection );
        raftState.update( outcomeB.build() );

        assertFalse( raftState.areTransferringLeadership() );
    }

    @Test
    void shouldDeactivateLeadershipTransferTimerIfNoLongerLeader() throws IOException
    {
        // given
        var timer = new ExpiringSet<RaftMemberId>( Duration.ofSeconds( 2 ), new FakeClock() );

        var raftState = new RaftState( raftMember( 0 ),
                new InMemoryStateStorage<>( new TermState() ),
                new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState() ),
                new ConsecutiveInFlightCache(), NullLogProvider.getInstance(), timer );

        assertFalse( raftState.areTransferringLeadership() );

        // when
        var outcomeA = OutcomeBuilder.builder( LEADER, raftState );
        outcomeA.startTransferringLeadership( raftMember( 1 ) );
        raftState.update( outcomeA.build() );
        outcomeA.startTransferringLeadership( raftMember( 2 ) );
        raftState.update( outcomeA.build() );

        // then
        assertTrue( raftState.areTransferringLeadership() );

        // when
        var outcomeB = OutcomeBuilder.builder( FOLLOWER, raftState );
        raftState.update( outcomeB.build() );

        assertFalse( raftState.areTransferringLeadership() );
    }

    private Collection<RaftMessages.Directed> emptyOutgoingMessages()
    {
        return new ArrayList<>();
    }

    private FollowerStates<RaftMemberId> initialFollowerStates()
    {
        return new FollowerStates<>( new FollowerStates<>(), raftMember( 1 ), new FollowerState() );
    }

    private Collection<RaftLogCommand> emptyLogCommands()
    {
        return Collections.emptyList();
    }

    private static class FakeMembership implements RaftMembership
    {
        @Override
        public Set<RaftMemberId> votingMembers()
        {
            return emptySet();
        }

        @Override
        public Set<RaftMemberId> replicationMembers()
        {
            return emptySet();
        }

        @Override
        public void registerListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }
    }
}
