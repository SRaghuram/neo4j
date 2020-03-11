/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.ElectionTimerMode;
import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.preVoteRequest;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.preVoteResponse;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteRequest;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteResponse;
import static com.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static com.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.assertj.core.api.Assertions.assertThat;

class CandidateTest
{
    private MemberId myself = member( 0 );
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    private LogProvider logProvider = NullLogProvider.getInstance();

    @Test
    void shouldImmediatelyHandleRejectionMessageOnLeadershipTransferProposal() throws Exception
    {
        // given
        var state = RaftStateBuilder.builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder()
                        .setVotesForMe( Set.of( myself, member1, member2 ) )
                        .setCommitIndex( 2 )
                        .setTerm( 1 ).build() )
                .supportsPreVoting( true )
                .build();

        var message = new RaftMessages.LeadershipTransfer.Proposal( myself, member1 );

        // when
        Outcome outcome = new Follower().handle( message, state, log() );

        // then
        var leaderTransferRejection = outcome.getLeaderTransferRejection();
        assertThat( leaderTransferRejection ).isNotNull();
    }

    @Test
    void shouldRespondWithRejectionOnLeaderTransferRequest() throws Exception
    {
        // given
        var state = RaftStateBuilder.builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder()
                        .setVotesForMe( Set.of( myself, member1, member2 ) )
                        .setCommitIndex( 3 )
                        .setTerm( 1 ).build() )
                .supportsPreVoting( true )
                .build();

        var message = new RaftMessages.LeadershipTransfer.Request( member2, 3, 1, Set.of() );

        // when
        Outcome outcome = new Candidate().handle( message, state, log() );

        // then
        assertThat( RaftMessages.Type.LEADERSHIP_TRANSFER_REJECTION ).isEqualTo( messageFor( outcome, member2 ).type() );
    }

    @Test
    void shouldBeElectedLeaderOnReceivingGrantedVoteResponseWithCurrentTerm() throws Exception
    {
        // given
        RaftState state = RaftStateBuilder.builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 1 ).build() )
                .myself( myself )
                .votingMembers( member1, member2 )
                .replicationMembers( member1, member2 )
                .build();

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( state.term() )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        Assertions.assertEquals( LEADER, outcome.getRole() );
        assertThat( outcome.electionTimerChanged() ).contains( ElectionTimerMode.FAILURE_DETECTION );
        assertThat( outcome.getLogCommands() ).contains( new AppendLogEntry( 0,
                new RaftLogEntry( state.term(), new NewLeaderBarrier() ) ) );
        assertThat( outcome.getOutgoingMessages() ).contains(
                new RaftMessages.Directed( member1, new RaftMessages.Heartbeat( myself, state.term(), -1, -1 ) ),
                new RaftMessages.Directed( member2, new RaftMessages.Heartbeat( myself, state.term(), -1, -1 ) )
        );
    }

    @Test
    void shouldStayAsCandidateOnReceivingDeniedVoteResponseWithCurrentTerm() throws Exception
    {
        // given
        RaftState state = newState();

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( state.term() )
                .from( member1 )
                .deny()
                .build(), state, log() );

        // then
        Assertions.assertEquals( CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldUpdateTermOnReceivingVoteResponseWithLaterTerm() throws Exception
    {
        // given
        RaftState state = newState();

        final long voterTerm = state.term() + 1;

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( voterTerm )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        Assertions.assertEquals( FOLLOWER, outcome.getRole() );
        Assertions.assertEquals( voterTerm, outcome.getTerm() );
    }

    @Test
    void shouldRejectVoteResponseWithOldTerm() throws Exception
    {
        // given
        RaftState state = newState();

        final long voterTerm = state.term() - 1;

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( voterTerm )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        Assertions.assertEquals( CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldDeclineVoteRequestsIfFromSameTerm() throws Throwable
    {
        // given
        RaftState raftState = newState();

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( raftState.term() )
                .build(), raftState, log() );

        // then
        assertThat(
                outcome.getOutgoingMessages() )
                .contains( new RaftMessages.Directed( member1, voteResponse().term( raftState.term() ).from( myself ).deny().build() ) );
        Assertions.assertEquals( Role.CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldBecomeFollowerIfReceiveVoteRequestFromLaterTerm() throws Throwable
    {
        // given
        RaftState raftState = newState();

        // when
        long newTerm = raftState.term() + 1;
        Outcome outcome = CANDIDATE.handler.handle( voteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( newTerm )
                .build(), raftState, log() );

        // then
        Assertions.assertEquals( newTerm, outcome.getTerm() );
        Assertions.assertEquals( Role.FOLLOWER, outcome.getRole() );
        assertThat( outcome.getVotesForMe() ).isEmpty();

        assertThat(
                outcome.getOutgoingMessages() ).contains( new RaftMessages.Directed( member1, voteResponse().term( newTerm ).from( myself ).grant().build() ) );
    }

    @Test
    void shouldDeclinePreVoteFromSameTerm() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .build();

        // when
        Outcome outcome = CANDIDATE.handler.handle( preVoteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( raftState.term() )
                .build(), raftState, log() );

        // then
        assertThat(
                outcome.getOutgoingMessages() )
                .contains( new RaftMessages.Directed( member1, preVoteResponse().term( raftState.term() ).from( myself ).deny().build() ) );
        Assertions.assertEquals( Role.CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldBecomeFollowerIfReceivePreVoteRequestFromLaterTerm() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .build();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = CANDIDATE.handler.handle( preVoteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( newTerm )
                .build(), raftState, log() );

        // then
        Assertions.assertEquals( newTerm, outcome.getTerm() );
        Assertions.assertEquals( Role.FOLLOWER, outcome.getRole() );
        assertThat( outcome.getVotesForMe() ).isEmpty();

        assertThat(
                outcome.getOutgoingMessages() )
                .contains( new RaftMessages.Directed( member1, preVoteResponse().term( newTerm ).from( myself ).deny().build() ) );
    }

    RaftState newState() throws IOException
    {
        return builder().myself( myself ).build();
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }

}
