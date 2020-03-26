/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import com.neo4j.causalclustering.core.consensus.schedule.OnDemandTimerService;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.causalclustering.messaging.Outbound;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteRequest;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteResponse;
import static com.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class ElectionTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    private Inbound inbound = mock( Inbound.class );
    private Outbound<MemberId,RaftMessages.RaftMessage> outbound = mock( Outbound.class );

    @Test
    void candidateShouldWinElectionAndBecomeLeader() throws Exception
    {
        // given
        FakeClock fakeClock = Clocks.fakeClock();
        TimerService timeouts = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .outbound( outbound )
                .timerService( timeouts )
                .clock( fakeClock )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 ) ) ) );
        raft.postRecoveryActions();

        timeouts.invoke( RaftMachine.Timeouts.ELECTION );

        // when
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );
        raft.handle( voteResponse().from( member2 ).term( 1 ).grant().build() );

        // then
        Assertions.assertEquals( 1, raft.term() );
        Assertions.assertEquals( LEADER, raft.currentRole() );

        /*
         * We require atLeast here because RaftMachine has its own scheduled service, which can spuriously wake up and
         * send empty entries. These are fine and have no bearing on the correctness of this test, but can cause it
         * fail if we expect exactly 2 of these messages
         */
        verify( outbound, atLeast( 1 ) ).send( eq( member1 ), isA( RaftMessages.AppendEntries.Request.class ) );
        verify( outbound, atLeast( 1 ) ).send( eq( member2 ), isA( RaftMessages.AppendEntries.Request.class ) );
    }

    @Test
    void candidateShouldLoseElectionAndRemainCandidate() throws Exception
    {
        // Note the etcd implementation seems to diverge from the paper here, since the paper suggests that it should
        // remain as a candidate

        // given
        FakeClock fakeClock = Clocks.fakeClock();
        TimerService timeouts = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .outbound( outbound )
                .timerService( timeouts )
                .clock( fakeClock )
                .build();

        raft.installCoreState(
                new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 ) ) ) );
        raft.postRecoveryActions();

        timeouts.invoke( RaftMachine.Timeouts.ELECTION );

        // when
        raft.handle( voteResponse().from( member1 ).term( 1 ).deny().build() );
        raft.handle( voteResponse().from( member2 ).term( 1 ).deny().build() );

        // then
        Assertions.assertEquals( 1, raft.term() );
        Assertions.assertEquals( CANDIDATE, raft.currentRole() );

        verify( outbound, never() ).send( eq( member1 ), isA( RaftMessages.AppendEntries.Request.class ) );
        verify( outbound, never() ).send( eq( member2 ), isA( RaftMessages.AppendEntries.Request.class ) );
    }

    @Test
    void candidateShouldVoteForTheSameCandidateInTheSameTerm() throws Exception
    {
        // given
        FakeClock fakeClock = Clocks.fakeClock();
        TimerService timeouts = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .outbound( outbound )
                .timerService( timeouts )
                .clock( fakeClock )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );

        // when
        raft.handle( voteRequest().from( member1 ).candidate( member1 ).term( 1 ).build() );
        raft.handle( voteRequest().from( member1 ).candidate( member1 ).term( 1 ).build() );

        // then
        verify( outbound, times( 2 ) ).send( member1, voteResponse().term( 1 ).grant().build() );
    }
}
