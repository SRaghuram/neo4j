/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.ElectionTimerMode;
import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.identity.RaftMemberId;
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
import static com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContextBuilder.contextWithState;
import static com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContextBuilder.contextWithStateWithPreVote;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.assertj.core.api.Assertions.assertThat;

class CandidateTest
{
    private RaftMemberId myself = raftMember( 0 );
    private RaftMemberId member1 = raftMember( 1 );
    private RaftMemberId member2 = raftMember( 2 );

    private LogProvider logProvider = NullLogProvider.getInstance();

    @Test
    void shouldImmediatelyHandleRejectionMessageOnLeadershipTransferProposal() throws Exception
    {
        // given
        var state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder()
                        .setCommitIndex( 2 )
                        .setTerm( 1 ).build() )
                .votingMembers( myself, member1, member2 )
                .build();

        var message = new RaftMessages.LeadershipTransfer.Proposal( myself, member1, Set.of() );

        // when
        Outcome outcome = new Follower().handle( message, contextWithStateWithPreVote( state ), log() );

        // then
        var leaderTransferRejection = outcome.getLeaderTransferRejection();
        assertThat( leaderTransferRejection ).isNotNull();
    }

    @Test
    void shouldRespondWithRejectionOnLeaderTransferRequest() throws Exception
    {
        // given
        var state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder()
                        .setCommitIndex( 3 )
                        .setTerm( 1 ).build() )
                .votingMembers( myself, member1, member2 )
                .build();

        var message = new RaftMessages.LeadershipTransfer.Request( member2, 3, 1, Set.of() );

        // when
        Outcome outcome = new Candidate().handle( message, contextWithStateWithPreVote( state ), log() );

        // then
        assertThat( RaftMessages.Type.LEADERSHIP_TRANSFER_REJECTION ).isEqualTo( messageFor( outcome, member2 ).type() );
    }

    @Test
    void shouldBeElectedLeaderOnReceivingGrantedVoteResponseWithCurrentTerm() throws Exception
    {
        // given
        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 1 ).build() )
                .myself( myself )
                .votingMembers( member1, member2 )
                .build();

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( state.term() )
                .from( member1 )
                .grant()
                .build(), contextWithState( state ), log() );

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
                .build(), contextWithState( state ), log() );

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
                .build(), contextWithState( state ), log() );

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
                .build(), contextWithState( state ), log() );

        // then
        Assertions.assertEquals( CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldDeclineVoteRequestsIfFromSameTerm() throws Throwable
    {
        // given
        RaftState state = newState();

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( state.term() )
                .build(), contextWithState( state ), log() );

        // then
        assertThat(
                outcome.getOutgoingMessages() )
                .contains( new RaftMessages.Directed( member1, voteResponse().term( state.term() ).from( myself ).deny().build() ) );
        Assertions.assertEquals( Role.CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldBecomeFollowerIfReceiveVoteRequestFromLaterTerm() throws Throwable
    {
        // given
        RaftState state = newState();

        // when
        long newTerm = state.term() + 1;
        Outcome outcome = CANDIDATE.handler.handle( voteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( newTerm )
                .build(), contextWithState( state ), log() );

        // then
        Assertions.assertEquals( newTerm, outcome.getTerm() );
        Assertions.assertEquals( Role.FOLLOWER, outcome.getRole() );
        assertThat( outcome.getVotesForMe() ).isEmpty();

        assertThat(
                outcome.getOutgoingMessages() ).contains( new RaftMessages.Directed( member1, voteResponse().term( newTerm ).from( myself ).grant().build() ) );
    }

    @Test
    void shouldGrantPreVoteFromSameTermAndSameLog() throws Throwable
    {
        // given
        var state = builder()
                .myself( myself )
                .build();

        // when
        var outcome = CANDIDATE.handler.handle( preVoteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( state.term() )
                .build(), contextWithStateWithPreVote( state ), log() );

        // then
        assertThat(
                outcome.getOutgoingMessages() )
                .contains( new RaftMessages.Directed( member1, preVoteResponse().term( state.term() ).from( myself ).grant().build() ) );
        Assertions.assertEquals( Role.CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldDeclinePreVoteFromSameTermButNewerLog() throws Throwable
    {
        var entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 0 ) ) );
        entryLog.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1 ) ) );

        // given
        var state = builder()
                .myself( myself )
                .entryLog( entryLog )
                .build();

        // when
        var outcome = CANDIDATE.handler.handle( preVoteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( state.term() )
                .build(), contextWithStateWithPreVote( state ), log() );

        // then
        assertThat(
                outcome.getOutgoingMessages() )
                .contains( new RaftMessages.Directed( member1, preVoteResponse().term( state.term() ).from( myself ).deny().build() ) );
        Assertions.assertEquals( Role.CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldBecomeFollowerIfReceivePreVoteRequestFromLaterTermAndGrantPreVote() throws Throwable
    {
        // given
        var state = builder()
                .myself( myself )
                .build();
        long newTerm = state.term() + 1;

        // when
        var outcome = CANDIDATE.handler.handle( preVoteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( newTerm )
                .build(), contextWithStateWithPreVote( state ), log() );

        // then
        Assertions.assertEquals( newTerm, outcome.getTerm() );
        Assertions.assertEquals( Role.FOLLOWER, outcome.getRole() );
        assertThat( outcome.getVotesForMe() ).isEmpty();

        assertThat(
                outcome.getOutgoingMessages() )
                .contains( new RaftMessages.Directed( member1, preVoteResponse().term( newTerm ).from( myself ).grant().build() ) );
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
