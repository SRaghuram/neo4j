/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Election;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.membership.RaftTestMembers;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static com.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static com.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static com.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static com.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class FollowerTest
{
    private final MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private final MemberId member1 = member( 1 );
    private final MemberId member2 = member( 2 );
    private final MemberId member3 = member( 3 );
    private final MemberId member4 = member( 4 );

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
                .supportsPreVoting( true )
                .build();

        var message = new RaftMessages.LeadershipTransfer.Proposal( myself, member1, Set.of() );

        // when
        Outcome outcome = new Follower().handle( message, state, log() );

        // then
        var leaderTransferRejection = outcome.getLeaderTransferRejection();
        assertThat( leaderTransferRejection ).isNotNull();
    }

    @Test
    void shouldInstigateAnElectionAfterValidLeadershipTransfer() throws Exception
    {
        // given
        var entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 1, new ReplicatedString( "foo" ) ) );
        var state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 1 ).build() )
                .entryLog( entryLog )
                .votingMembers( myself, member1, member2 )
                .supportsPreVoting( true )
                .build();

        var message = new RaftMessages.LeadershipTransfer.Request( member2, 3, 1, Set.of() );
        var follower = new Follower();
        appendSomeEntriesToLog( state, follower, 3, 1, 1 );

        // when
        Outcome outcome = follower.handle( message, state, log() );

        // then
        assertThat( messageFor( outcome, member1 ).type() ).isEqualTo( RaftMessages.Type.VOTE_REQUEST );
        assertThat( messageFor( outcome, member2 ).type() ).isEqualTo( RaftMessages.Type.VOTE_REQUEST );
        assertThat( outcome.getOutgoingMessages() ).hasSize( 2 );
    }

    @Test
    void shouldSendALeadershipRejectionResponseAfterLeadershipTransferWithUnseenIndex() throws Exception
    {
        // given
        var entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 1, new ReplicatedString( "foo" ) ) );
        var state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 1 ).build() )
                .votingMembers( myself, member1, member2 )
                .entryLog( entryLog )
                .supportsPreVoting( true )
                .build();

        var follower = new Follower();
        appendSomeEntriesToLog( state, follower, 2, 1, 1 );

        var message = new RaftMessages.LeadershipTransfer.Request( member2, 3, 1, Set.of() );

        // when
        Outcome outcome = new Follower().handle( message, state, log() );

        // then
        assertThat( messageFor( outcome, member2 ).type() ).isEqualTo( RaftMessages.Type.LEADERSHIP_TRANSFER_REJECTION );
        assertThat( outcome.getOutgoingMessages() ).hasSize( 1 );

        // when
        appendSomeEntriesToLog( state, follower, 1, 1, 3 );
        outcome = follower.handle( message, state, log() );

        // then
        assertThat( messageFor( outcome, member1 ).type() ).isEqualTo( RaftMessages.Type.VOTE_REQUEST );
        assertThat( messageFor( outcome, member2 ).type() ).isEqualTo( RaftMessages.Type.VOTE_REQUEST );
        assertThat( outcome.getOutgoingMessages() ).hasSize( 2 );
    }

    @Test
    void shouldSendALeadershipRejectionResponseAfterLeadershipTransferWithDifferentTerm() throws Exception
    {

        // given
        var entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 1, new ReplicatedString( "foo" ) ) );
        var state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 1 ).build() )
                .votingMembers( myself, member1, member2 )
                .entryLog( entryLog )
                .supportsPreVoting( true )
                .build();

        var follower = new Follower();
        appendSomeEntriesToLog( state, follower, 2, 1, 1 );

        var message = new RaftMessages.LeadershipTransfer.Request( member2, 2, 2, Set.of() );

        // when
        Outcome outcome = new Follower().handle( message, state, log() );

        // then
        assertThat( messageFor( outcome, member2 ).type() ).isEqualTo( RaftMessages.Type.LEADERSHIP_TRANSFER_REJECTION );
        assertThat( outcome.getOutgoingMessages() ).hasSize( 1 );
    }

    @Test
    void shouldSendALeadershipRejectionResponseAfterLeadershipTransferIfRefusingToBeLeader() throws Exception
    {
        // given
        var validLeadershipTransfer = new ValidLeadershipTransfer();
        var state = validLeadershipTransfer.stateBuilder
                .setRefusesToBeLeader( true )
                .build();

        var message = validLeadershipTransfer.message;

        // when
        Outcome outcome = new Follower().handle( message, state, log() );

        // then
        assertThat( messageFor( outcome, member2 ).type() ).isEqualTo( RaftMessages.Type.LEADERSHIP_TRANSFER_REJECTION );
        assertThat( outcome.getOutgoingMessages() ).hasSize( 1 );
    }

    @Test
    void shouldInstigateAnElectionAfterTimeout() throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        Assertions.assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome, member1 ).type() );
        Assertions.assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome, member2 ).type() );
    }

    @Test
    void shouldBecomeCandidateOnReceivingElectionTimeoutMessage() throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        Assertions.assertEquals( CANDIDATE, outcome.getRole() );
    }

    @Test
    void shouldNotInstigateElectionOnElectionTimeoutIfRefusingToBeLeaderAndPreVoteNotSupported() throws Throwable
    {
        // given
        RaftState state = builder()
                .setRefusesToBeLeader( true )
                .supportsPreVoting( false )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertThat( outcome.getOutgoingMessages() ).isEmpty();
    }

    @Test
    void shouldIgnoreAnElectionTimeoutIfRefusingToBeLeaderAndPreVoteNotSupported() throws Throwable
    {
        // given
        RaftState state = builder()
                .setRefusesToBeLeader( true )
                .supportsPreVoting( false )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        Assertions.assertEquals( OutcomeBuilder.builder( Role.FOLLOWER, state ).build(), outcome );
    }

    @Test
    void shouldSetPreElectionOnTimeoutIfSupportedAndIAmVoterAndIRefuseToLead() throws Throwable
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .votingMembers( myself, member1, member2 )
                .setRefusesToBeLeader( true )
                .supportsPreVoting( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        Assertions.assertTrue( outcome.isPreElection() );
    }

    @Test
    void shouldNotSetPreElectionOnTimeoutIfSupportedAndIAmNotVoterAndIRefuseToLead() throws Throwable
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .votingMembers( member1, member2, member3 )
                .setRefusesToBeLeader( true )
                .supportsPreVoting( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        Assertions.assertFalse( outcome.isPreElection() );
    }

    @Test
    void shouldNotSolicitPreVotesOnTimeoutEvenIfSupportedIfRefuseToLead() throws Throwable
    {
        // given
        RaftState state = builder()
                .setRefusesToBeLeader( true )
                .supportsPreVoting( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertThat( outcome.getOutgoingMessages() ).isEmpty();
    }

    @Test
    void followerReceivingHeartbeatIndicatingClusterIsAheadShouldElicitAppendResponse() throws Exception
    {
        // given
        int term = 1;
        int followerAppendIndex = 9;
        RaftLog entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, new RaftTestMembers( 0 ) ) );
        RaftState state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( term ).build() )
                .build();

        Follower follower = new Follower();
        appendSomeEntriesToLog( state, follower, followerAppendIndex - 1, term, 1 );

        AppendEntries.Request heartbeat = appendEntriesRequest().from( member1 )
                .leaderTerm( term )
                .prevLogIndex( followerAppendIndex + 2 ) // leader has appended 2 ahead from this follower
                .prevLogTerm( term ) // in the same term
                .build(); // no entries, this is a heartbeat

        Outcome outcome = follower.handle( heartbeat, state, log() );

        Assertions.assertEquals( 1, outcome.getOutgoingMessages().size() );
        RaftMessage outgoing = outcome.getOutgoingMessages().iterator().next().message();
        Assertions.assertEquals( RaftMessages.Type.APPEND_ENTRIES_RESPONSE, outgoing.type() );
        RaftMessages.AppendEntries.Response response = (AppendEntries.Response) outgoing;
        Assertions.assertFalse( response.success() );
    }

    @Test
    void shouldTruncateIfTermDoesNotMatch() throws Exception
    {
        // given
        RaftLog entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, new RaftTestMembers( 0 ) ) );
        int term = 1;
        RaftState state = builder()
                .myself( myself )
                .entryLog( entryLog )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( term ).build() )
                .build();

        Follower follower = new Follower();

        state.update( follower.handle( new AppendEntries.Request( member1, 1, 0, 0,
                new RaftLogEntry[]{
                        new RaftLogEntry( 2, ContentGenerator.content() ),
                },
                0 ), state, log() ) );

        RaftLogEntry[] entries = {
                new RaftLogEntry( 1, new ReplicatedString( "commit this!" ) ),
        };

        Outcome outcome = follower.handle(
                new AppendEntries.Request( member1, 1, 0, 0, entries, 0 ), state, log() );
        state.update( outcome );

        // then
        Assertions.assertEquals( 1, state.entryLog().appendIndex() );
        Assertions.assertEquals( 1, state.entryLog().readEntryTerm( 1 ) );
    }

    // TODO move this to outcome tests
    @Test
    void followerLearningAboutHigherCommitCausesValuesTobeAppliedToItsLog() throws Exception
    {
        // given
        RaftLog entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, new RaftTestMembers( 0 ) ) );
        RaftState state = builder()
                .myself( myself )
                .entryLog( entryLog )
                .build();

        Follower follower = new Follower();

        appendSomeEntriesToLog( state, follower, 3, 0, 1 );

        // when receiving AppEntries with high leader commit (4)
        Outcome outcome = follower.handle( new AppendEntries.Request( myself, 0, 3, 0,
                new RaftLogEntry[] { new RaftLogEntry( 0, ContentGenerator.content() ) }, 4 ), state, log() );

        state.update( outcome );

        // then
        Assertions.assertEquals( 4, state.commitIndex() );
    }

    @Test
    void shouldUpdateCommitIndexIfNecessary() throws Exception
    {
        //  If leaderCommit > commitIndex, set commitIndex = min( leaderCommit, index of last new entry )

        // given
        RaftLog entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, new RaftTestMembers( 0 ) ) );
        RaftState state = builder()
                .myself( myself )
                .entryLog( entryLog )
                .build();

        Follower follower = new Follower();

        int localAppendIndex = 3;
        int localCommitIndex =  localAppendIndex - 1;
        int term = 0;
        appendSomeEntriesToLog( state, follower, localAppendIndex, term, 1 ); // append index is 0 based

        // the next when-then simply verifies that the test is setup properly, with commit and append index as expected
        // when
        OutcomeBuilder raftTestMemberOutcome = OutcomeBuilder.builder( FOLLOWER, state );
        raftTestMemberOutcome.setCommitIndex( localCommitIndex );
        state.update( raftTestMemberOutcome.build() );

        // then
        Assertions.assertEquals( localAppendIndex, state.entryLog().appendIndex() );
        Assertions.assertEquals( localCommitIndex, state.commitIndex() );

        // when
        // an append req comes in with leader commit index > localAppendIndex but localCommitIndex < localAppendIndex
        Outcome outcome = follower.handle( appendEntriesRequest()
                .leaderTerm( term ).prevLogIndex( 3 )
                .prevLogTerm( term ).leaderCommit( localCommitIndex + 4 )
                .build(), state, log() );

        state.update( outcome );

        // then
        // The local commit index must be brought as far along as possible
        Assertions.assertEquals( 3, state.commitIndex() );
    }

    @Test
    void shouldNotRenewElectionTimeoutOnReceiptOfHeartbeatInLowerTerm() throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 2 ).build() )
                .build();

        Follower follower = new Follower();

        Outcome outcome = follower.handle( new RaftMessages.Heartbeat( myself, 1, 1, 1 ),
                state, log() );

        // then
        assertThat( outcome.electionTimerChanged() ).isEmpty();
    }

    @Test
    void shouldAcknowledgeHeartbeats() throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 2 ).build() )
                .build();

        Follower follower = new Follower();

        Outcome outcome = follower.handle( new RaftMessages.Heartbeat( state.leader(), 2, 2, 2 ),
                state, log() );

        // then
        Collection<RaftMessages.Directed> outgoingMessages = outcome.getOutgoingMessages();
        Assertions.assertTrue( outgoingMessages.contains( new RaftMessages.Directed( state.leader(),
                new RaftMessages.HeartbeatResponse( myself ) ) ) );
    }

    @Test
    void shouldRespondPositivelyToPreVoteRequestsIfWouldVoteForCandidate() throws Exception
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_RESPONSE );
        assertThat( ((RaftMessages.PreVote.Response) raftMessage).voteGranted() ).isTrue();
    }

    @Test
    void shouldRespondPositivelyToPreVoteRequestsEvenIfAlreadyVotedInRealElection() throws Exception
    {
        // given
        RaftState raftState = preElectionActive();
        raftState.update( new Follower().handle( new RaftMessages.Vote.Request( member1, 0, member1, 0, 0 ), raftState, log() ) );

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member2, 0, member2, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member2 );
        assertThat( raftMessage.type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_RESPONSE );
        assertThat( ((RaftMessages.PreVote.Response) raftMessage).voteGranted() ).isTrue();
    }

    @Test
    void shouldRespondNegativelyToPreVoteRequestsIfNotInPreVoteMyself() throws Exception
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_RESPONSE );
        assertThat( ((RaftMessages.PreVote.Response) raftMessage).voteGranted() ).isFalse();
    }

    @Test
    void shouldRespondToPreVoteRequestsIfTimersAreNotYetStarted() throws Exception
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .setBeforeTimersStarted( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_RESPONSE );
        assertThat( ((RaftMessages.PreVote.Response) raftMessage).voteGranted() ).isTrue();
    }

    @Test
    void shouldRespondNegativelyToPreVoteRequestsIfWouldNotVoteForCandidate() throws Exception
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 1 ).setPreElection( true ).build() )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_RESPONSE );
        assertThat( ((RaftMessages.PreVote.Response) raftMessage).voteGranted() ).isFalse();
    }

    @Test
    void shouldRespondPositivelyToPreVoteRequestsToMultipleMembersIfWouldVoteForAny() throws Exception
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome1 = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );
        raftState.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Request( member2, 0, member2, 0, 0 ), raftState, log() );
        raftState.update( outcome2 );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome2, member2 );

        assertThat( raftMessage.type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_RESPONSE );
        assertThat( ((RaftMessages.PreVote.Response) raftMessage).voteGranted() ).isTrue();
    }

    @Test
    void shouldUseTermFromPreVoteRequestIfHigherThanOwn() throws Exception
    {
        // given
        RaftState raftState = preElectionActive();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, newTerm, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );

        assertThat( raftMessage.type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_RESPONSE );
        assertThat( ((RaftMessages.PreVote.Response) raftMessage).term() ).isEqualTo( newTerm );
    }

    @Test
    void shouldUpdateOutcomeWithTermFromPreVoteRequestOfLaterTermIfInPreVoteState() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, newTerm, member1, 0, 0 ), raftState, log() );

        // then
        Assertions.assertEquals( newTerm, outcome.getTerm() );
    }

    @Test
    void shouldUpdateOutcomeWithTermFromPreVoteRequestOfLaterTermIfNotInPreVoteState() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .addInitialOutcome( OutcomeTestBuilder.builder().setPreElection( false ).build() )
                .build();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, newTerm, member1, 0, 0 ), raftState, log() );

        // then

        Assertions.assertEquals( newTerm, outcome.getTerm() );
    }

    @Test
    void shouldInstigatePreElectionIfSupportedAndNotActiveAndReceiveTimeout() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .supportsPreVoting( true )
                .addInitialOutcome( OutcomeTestBuilder.builder().setPreElection( false ).build() )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), raftState, log() );

        // then
        Assertions.assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member1 ).type() );
        Assertions.assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member2 ).type() );
    }

    @Test
    void shouldSetPreElectionActiveWhenReceiveTimeout() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .supportsPreVoting( true )
                .addInitialOutcome( OutcomeTestBuilder.builder().setPreElection( false ).build() )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), raftState, log() );

        // then
        Assertions.assertTrue( outcome.isPreElection() );
    }

    @Test
    void shouldInstigatePreElectionIfSupportedAndActiveAndReceiveTimeout() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), raftState, log() );

        // then
        Assertions.assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member1 ).type() );
        Assertions.assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member2 ).type() );
        Assertions.assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member3 ).type() );
    }

    @Test
    void shouldKeepPreElectionActiveWhenReceiveTimeout() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), raftState, log() );

        // then
        Assertions.assertTrue( outcome.isPreElection() );
    }

    @Test
    void shouldAbortPreElectionIfReceivePreVoteResponseFromNewerTerm() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, newTerm, false ), raftState, log() );

        // then
        Assertions.assertEquals( newTerm, outcome.getTerm() );
        Assertions.assertFalse( outcome.isPreElection() );
    }

    @Test
    void shouldIgnoreVotesFromEarlierTerms() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();
        long oldTerm = raftState.term() - 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, oldTerm, true ), raftState, log() );

        // then
        Assertions.assertEquals( OutcomeBuilder.builder( Role.FOLLOWER, raftState ).build(), outcome );
    }

    @Test
    void shouldIgnoreVotesDeclining() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), false ), raftState, log() );

        // then
        Assertions.assertEquals( OutcomeBuilder.builder( Role.FOLLOWER, raftState ).build(), outcome );
    }

    @Test
    void shouldAddVoteFromADifferentMember() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        assertThat( outcome.getPreVotesForMe() ).contains( member1 );
    }

    @Test
    void shouldNotAddVoteFromMyself() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( myself, raftState.term(), true ), raftState, log() );

        // then
        assertThat( outcome.getPreVotesForMe() ).doesNotContain( member1 );
    }

    @Test
    void shouldNotStartElectionIfHaveNotReachedQuorum() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        Assertions.assertEquals( Role.FOLLOWER, outcome.getRole() );
    }

    @Test
    void shouldTransitionToCandidateIfHaveReachedQuorum() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome1 = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );
        raftState.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Response( member2, raftState.term(), true ), raftState, log() );

        // then
        Assertions.assertEquals( Role.CANDIDATE, outcome2.getRole() );
    }

    @Test
    void shouldInstigateElectionIfHaveReachedQuorum() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome1 = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );
        raftState.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Response( member2, raftState.term(), true ), raftState, log() );

        // then
        Assertions.assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome2, member1 ).type() );
        Assertions.assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome2, member2 ).type() );
        Assertions.assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome2, member3 ).type() );
    }

    @Test
    void shouldIgnorePreVoteResponsesIfPreVoteInactive() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        Assertions.assertEquals( OutcomeBuilder.builder( Role.FOLLOWER, raftState ).build(), outcome );
    }

    @Test
    void shouldIgnorePreVoteRequestsIfPreVoteUnsupported() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( false )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, raftState.term(), member1, 0, 0 ), raftState, log() );

        Assertions.assertEquals( OutcomeBuilder.builder( Role.FOLLOWER, raftState ).build(), outcome );
    }

    @Test
    void shouldIgnorePreVoteResponsesIfPreVoteUnsupported() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( false )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        Assertions.assertEquals( OutcomeBuilder.builder( Role.FOLLOWER, raftState ).build(), outcome );
    }

    @Test
    void shouldIgnorePreVoteResponseWhenPreElectionFalseRefuseToBeLeader() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        Assertions.assertEquals( OutcomeBuilder.builder( Role.FOLLOWER, raftState ).build(), outcome );
    }

    @Test
    void shouldIgnorePreVoteResponseWhenPreElectionTrueAndRefuseLeader() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setPreElection( true ).build() )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        Assertions.assertEquals( OutcomeBuilder.builder( Role.FOLLOWER, raftState ).build(), outcome );
    }

    @Test
    void shouldNotInstigateElectionOnPreVoteResponseWhenPreElectionTrueAndRefuseLeader() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setPreElection( true ).build() )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        assertThat( outcome.getOutgoingMessages() ).isEmpty();
    }

    @Test
    void shouldDeclinePreVoteRequestsIfPreElectionNotActiveAndRefusesToLead() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, raftState.term(), member1, 0, 0 ), raftState, log() );

        // then
        Assertions.assertFalse( ((RaftMessages.PreVote.Response) messageFor( outcome, member1 )).voteGranted() );
    }

    @Test
    void shouldApprovePreVoteRequestIfPreElectionActiveAndRefusesToLead() throws Throwable
    {
        // given
        RaftState raftState = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setPreElection( true ).build() )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, raftState.term(), member1, 0, 0 ), raftState, log() );

        // then
        Assertions.assertTrue( ((RaftMessages.PreVote.Response) messageFor( outcome, member1 )).voteGranted() );
    }

    @Test
    void shouldSetPreElectionOnElectionTimeout() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome );

        // then
        assertThat( outcome.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome.isPreElection() ).isTrue();
    }

    @Test
    void shouldSendPreVoteRequestsOnElectionTimeout() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome );

        // then
        assertThat( messageFor( outcome, member1 ).type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_REQUEST );
        assertThat( messageFor( outcome, member2 ).type() ).isEqualTo( RaftMessages.Type.PRE_VOTE_REQUEST );
    }

    @Test
    void shouldProceedToRealElectionIfReceiveQuorumOfPositivePreVoteResponses() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole() ).isEqualTo( CANDIDATE );
        assertThat( outcome2.isPreElection() ).isFalse();
        assertThat( outcome2.getPreVotesForMe() ).contains( member1 );
    }

    @Test
    void shouldIgnorePreVotePositiveResponsesFromOlderTerm() throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 1 ).build() )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome2.isPreElection() ).isTrue();
        assertThat( outcome2.getPreVotesForMe() ).isEmpty();
    }

    @Test
    void shouldIgnorePositivePreVoteResponsesIfNotInPreVotingStage() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        // when
        Outcome outcome = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome.isPreElection() ).isFalse();
        assertThat( outcome.getPreVotesForMe() ).isEmpty();
    }

    @Test
    void shouldNotMoveToRealElectionWithoutPreVoteQuorum() throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome2.isPreElection() ).isTrue();
        assertThat( outcome2.getPreVotesForMe() ).contains( member1 );
    }

    @Test
    void shouldMoveToRealElectionWithPreVoteQuorumOf5() throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, true ), state, log() );

        // then
        assertThat( outcome3.getRole() ).isEqualTo( CANDIDATE );
        assertThat( outcome3.isPreElection() ).isFalse();
    }

    @Test
    void shouldNotCountPreVotesVotesFromSameMemberTwice() throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome3.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome3.isPreElection() ).isTrue();
    }

    @Test
    void shouldResetPreVotesWhenMovingBackToFollower() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Outcome outcome1 = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        assertThat( outcome2.getRole() ).isEqualTo( CANDIDATE );
        assertThat( outcome2.getPreVotesForMe() ).contains( member1 );

        // when
        Outcome outcome3 = new Candidate().handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome3.getPreVotesForMe() ).isEmpty();
    }

    @Test
    void shouldSendRealVoteRequestsIfReceivePositivePreVoteResponses() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( messageFor( outcome2, member1 ).type() ).isEqualTo( RaftMessages.Type.VOTE_REQUEST );
        assertThat( messageFor( outcome2, member2 ).type() ).isEqualTo( RaftMessages.Type.VOTE_REQUEST );
    }

    @Test
    void shouldNotProceedToRealElectionIfReceiveNegativePreVoteResponses() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, false ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, false ), state, log() );

        // then
        assertThat( outcome3.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome3.isPreElection() ).isTrue();
        assertThat( outcome3.getPreVotesForMe() ).isEmpty();
    }

    @Test
    void shouldNotSendRealVoteRequestsIfReceiveNegativePreVoteResponses() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, false ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, false ), state, log() );

        // then
        assertThat( outcome2.getOutgoingMessages() ).isEmpty();
        assertThat( outcome3.getOutgoingMessages() ).isEmpty();
    }

    @Test
    void shouldResetPreVoteIfReceiveHeartbeatFromLeader() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.Heartbeat( member1, 0L, 0L, 0L ), state, log() );

        // then
        assertThat( outcome2.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome2.isPreElection() ).isFalse();
        assertThat( outcome2.getPreVotesForMe() ).isEmpty();
    }

    @Test
    void shouldResetPreVoteIfReceiveAppendEntriesRequestFromLeader() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle(
                appendEntriesRequest().leaderTerm( state.term() ).prevLogTerm( state.term() ).prevLogIndex( 0 ).build(),
                state, log() );

        // then
        assertThat( outcome2.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome2.isPreElection() ).isFalse();
        assertThat( outcome2.getPreVotesForMe() ).isEmpty();
    }

    private RaftState preElectionActive() throws IOException
    {
        return builder()
                .myself( myself )
                .supportsPreVoting( true )
                .addInitialOutcome( OutcomeTestBuilder.builder().setPreElection( true ).build() )
                .votingMembers( asSet( myself, member1, member2, member3 ) )
                .build();
    }

    private RaftState preElectionSupported() throws IOException
    {
        return builder()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();
    }

    private void appendSomeEntriesToLog( RaftState raft, Follower follower, int numberOfEntriesToAppend, int term,
            int firstIndex ) throws IOException
    {
        for ( int i = 0; i < numberOfEntriesToAppend; i++ )
        {
            int prevLogIndex = (firstIndex + i) - 1;
            raft.update( follower.handle( new AppendEntries.Request( myself, term, prevLogIndex, term,
                    new RaftLogEntry[]{new RaftLogEntry( term, ContentGenerator.content() )}, -1 ), raft, log() ) );
        }
    }

    private static class ContentGenerator
    {
        private static int count;

        public static ReplicatedString content()
        {
            return new ReplicatedString( String.format( "content#%d", count++ ) );
        }
    }

    private Log log()
    {
        return NullLog.getInstance();
    }

    private class ValidLeadershipTransfer
    {
        private final RaftStateBuilder stateBuilder;
        private final RaftMessages.LeadershipTransfer.Request message;

        ValidLeadershipTransfer()
        {
            stateBuilder = builder()
                    .myself( myself )
                    .addInitialOutcome( OutcomeTestBuilder.builder()
                            .setCommitIndex( 3 )
                            .setTerm( 1 )
                            .build() )
                    .votingMembers( myself, member1, member2 )
                    .supportsPreVoting( true );

            message = new RaftMessages.LeadershipTransfer.Request( member2, 3, 1, Set.of() );
        }
    }
}
