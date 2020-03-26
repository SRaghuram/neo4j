/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import com.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Heartbeat;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.BatchAppendLogEntries;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.ShipCommand;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesResponse;
import static com.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

@RunWith( MockitoJUnitRunner.class )
public class LeaderTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    private LogProvider logProvider = NullLogProvider.getInstance();

    private static final ReplicatedString CONTENT = ReplicatedString.valueOf( "some-content-to-raft" );

    @Test
    public void leaderShouldNotRespondToSuccessResponseFromFollowerThatWillSoonUpToDateViaInFlightMessages()
            throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is at an index less than 100 -say 84 but it has already been sent up to 100
         */
        Leader leader = new Leader();
        MemberId instance2 = member( 2 );
        FollowerState instance2State = createArtificialFollowerState( 84 );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100L );

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4L ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is up to date
        RaftMessages.AppendEntries.Response response =
                appendEntriesResponse().success().matchIndex( 90 ).term( 4 ).from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        // The leader should not be trying to send any messages to that instance
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // And the follower state should be updated
        FollowerStates<MemberId> leadersViewOfFollowerStates = outcome.getFollowerStates();
        assertEquals( 90, leadersViewOfFollowerStates.get( instance2 ).getMatchIndex() );
    }

    @Test
    public void leaderShouldNotRespondToSuccessResponseThatIndicatesUpToDateFollower() throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is at an index less than 100 -say 84
         */
        Leader leader = new Leader();
        MemberId instance2 = member( 2 );
        FollowerState instance2State = createArtificialFollowerState( 84 );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100L );

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4L ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is up to date
        RaftMessages.AppendEntries.Response response =
                appendEntriesResponse().success().matchIndex( 100 ).term( 4 ).from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        // The leader should not be trying to send any messages to that instance
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // And the follower state should be updated
        FollowerStates<MemberId> updatedFollowerStates = outcome.getFollowerStates();
        assertEquals( 100, updatedFollowerStates.get( instance2 ).getMatchIndex() );
    }

    @Test
    public void leaderShouldRespondToSuccessResponseThatIndicatesLaggingFollowerWithJustWhatItsMissing()
            throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is at an index less than 100 -say 50
         */
        Leader leader = new Leader();
        MemberId instance2 = member( 2 );
        FollowerState instance2State = createArtificialFollowerState( 50 );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100L );
        // with commit requests in this test

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 231L ); // both leader and follower are in the same term

        // when that leader is asked to handle a response from that follower that says that the follower is still
        // missing things
        RaftMessages.AppendEntries.Response response = appendEntriesResponse()
                .success()
                .matchIndex( 89 )
                .term( 231 )
                .from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        int matchCount = 0;
        for ( ShipCommand shipCommand : outcome.getShipCommands() )
        {
            if ( shipCommand instanceof ShipCommand.Match )
            {
                matchCount++;
            }
        }

        assertThat( matchCount ).isGreaterThan( 0 );
    }

    @Test
    public void leaderShouldIgnoreSuccessResponseThatIndicatesLaggingWhileLocalStateIndicatesFollowerIsCaughtUp()
            throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is fully caught up
         */
        Leader leader = new Leader();
        MemberId instance2 = member( 2 );
        int j = 100;
        FollowerState instance2State = createArtificialFollowerState( j );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100L );
        //  with commit requests in this test

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4L ); // both leader and follower are in the same term

        // when that leader is asked to handle a response from that follower that says that the follower is still
        // missing things
        RaftMessages.AppendEntries.Response response = appendEntriesResponse()
                .success()
                .matchIndex( 80 )
                .term( 4 )
                .from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then the leader should not send anything, since this is a delayed, out of order response to a previous append
        // request
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // The follower state should not be touched
        FollowerStates<MemberId> updatedFollowerStates = outcome.getFollowerStates();
        assertEquals( 100, updatedFollowerStates.get( instance2 ).getMatchIndex() );
    }

    private static FollowerState createArtificialFollowerState( long matchIndex )
    {
        return new FollowerState().onSuccessResponse( matchIndex );
    }

    // TODO: rethink this test, it does too much
    @Test
    public void leaderShouldSpawnMismatchCommandOnFailure() throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is fully caught up
         */
        Leader leader = new Leader();
        MemberId instance2 = member( 2 );
        FollowerState instance2State = createArtificialFollowerState( 100 );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        RaftLog log = new InMemoryRaftLog();
        for ( int i = 0; i <= 100; i++ )
        {
            log.append( new RaftLogEntry( 0, valueOf( i ) ) );
        }

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( log );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4L ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is still missing
        // things
        RaftMessages.AppendEntries.Response response = appendEntriesResponse()
                .failure()
                .appendIndex( 0 )
                .matchIndex( -1 )
                .term( 4 )
                .from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        int mismatchCount = 0;
        for ( ShipCommand shipCommand : outcome.getShipCommands() )
        {
            if ( shipCommand instanceof ShipCommand.Mismatch )
            {
                mismatchCount++;
            }
        }

        assertThat( mismatchCount ).isGreaterThan( 0 );
    }

    @Test
    public void shouldSendCompactionInfoIfFailureWithNoEarlierEntries() throws Exception
    {
        // given
        Leader leader = new Leader();
        long term = 1;
        long leaderPrevIndex = 3;
        long followerIndex = leaderPrevIndex - 1;

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.skip( leaderPrevIndex, term );

        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( term ).build() )
                .entryLog( raftLog )
                .build();

        RaftMessages.AppendEntries.Response incomingResponse = appendEntriesResponse()
                .failure()
                .term( term )
                .appendIndex( followerIndex )
                .from( member1 ).build();

        // when
        Outcome outcome = leader.handle( incomingResponse, state, log() );

        // then
        RaftMessages.RaftMessage outgoingMessage = messageFor( outcome, member1 );
        assertThat( outgoingMessage ).isInstanceOf( RaftMessages.LogCompactionInfo.class );

        RaftMessages.LogCompactionInfo typedOutgoingMessage = (RaftMessages.LogCompactionInfo) outgoingMessage;
        assertThat( typedOutgoingMessage.prevIndex() ).isEqualTo( leaderPrevIndex );
    }

    @Test
    public void shouldIgnoreAppendResponsesFromOldTerms() throws Exception
    {
        // given
        Leader leader = new Leader();
        long leaderTerm = 5;
        long followerTerm = 3;

        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( leaderTerm ).build() )
                .build();

        RaftMessages.AppendEntries.Response incomingResponse = appendEntriesResponse()
                .failure()
                .term( followerTerm )
                .from( member1 ).build();

        // when
        Outcome outcome = leader.handle( incomingResponse, state, log() );

        // then
        assertThat( outcome.getTerm() ).isEqualTo( leaderTerm );
        assertThat( outcome.getRole() ).isEqualTo( LEADER );

        assertThat( outcome.getOutgoingMessages() ).isEmpty();
        assertThat( outcome.getShipCommands() ).isEmpty();
    }

    @Test
    public void leaderShouldRejectAppendEntriesResponseWithNewerTermAndBecomeAFollower() throws Exception
    {
        // given
        RaftState state = builder().myself( myself ).build();

        Leader leader = new Leader();

        // when
        AppendEntries.Response message = appendEntriesResponse()
                .from( member1 )
                .term( state.term() + 1 )
                .build();
        Outcome outcome = leader.handle( message, state, log() );

        // then
        assertEquals( 0, count( outcome.getOutgoingMessages() ) );
        assertEquals( FOLLOWER, outcome.getRole() );
        assertEquals( 0, count( outcome.getLogCommands() ) );
        assertEquals( state.term() + 1, outcome.getTerm() );
    }

    // TODO: test that shows we don't commit for previous terms

    @Test
    public void leaderShouldSendHeartbeatsToAllClusterMembersOnReceiptOfHeartbeatTick() throws Exception
    {
        // given
        RaftState state = builder()
                .votingMembers( myself, member1, member2 )
                .replicationMembers( myself, member1, member2 )
                .build();

        Leader leader = new Leader();
        leader.handle( new RaftMessages.HeartbeatResponse( member1 ), state, log() ); // make sure it has quorum.

        // when
        Outcome outcome = leader.handle( new Heartbeat( myself ), state, log() );

        // then
        assertTrue( messageFor( outcome, member1 ) instanceof RaftMessages.Heartbeat );
        assertTrue( messageFor( outcome, member2 ) instanceof RaftMessages.Heartbeat );
    }

    @Test
    public void leaderShouldStepDownWhenLackingHeartbeatResponses() throws Exception
    {
        // given
        RaftState state = builder()
                .votingMembers( asSet( myself, member1, member2 ) )
                .addInitialOutcome( OutcomeTestBuilder.builder().setLeader( myself ).build() )
                .build();

        Leader leader = new Leader();
        leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // when
        Outcome outcome = leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome.getRole() ).isNotEqualTo( LEADER );
        assertNull( outcome.getLeader() );
    }

    @Test
    public void leaderShouldNotStepDownWhenReceivedQuorumOfHeartbeatResponses() throws Exception
    {
        // given
        RaftState state = builder()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        // when
        Outcome outcome = leader.handle( new RaftMessages.HeartbeatResponse( member1 ), state, log() );
        state.update( outcome );

        // we now have quorum and should not step down
        outcome = leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( LEADER );
    }

    @Test
    public void oldHeartbeatResponseShouldNotPreventStepdown() throws Exception
    {
        // given
        RaftState state = builder()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        Outcome outcome = leader.handle( new RaftMessages.HeartbeatResponse( member1 ), state, log() );
        state.update( outcome );

        outcome = leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome );

        assertThat( outcome.getRole() ).isEqualTo( LEADER );

        // when
        outcome = leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( FOLLOWER );
    }

    @Test
    public void leaderShouldDecideToAppendToItsLogAndSendAppendEntriesMessageOnReceiptOfClientProposal()
            throws Exception
    {
        // given
        RaftState state = builder()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        RaftMessages.NewEntry.Request newEntryRequest = new RaftMessages.NewEntry.Request( member( 9 ), CONTENT );

        // when
        Outcome outcome = leader.handle( newEntryRequest, state, log() );
        //state.update( outcome );

        // then
        AppendLogEntry logCommand = (AppendLogEntry) single( outcome.getLogCommands() );
        assertEquals( 0, logCommand.index );
        assertEquals( 0, logCommand.entry.term() );

        ShipCommand.NewEntries shipCommand = (ShipCommand.NewEntries) single( outcome.getShipCommands() );

        assertEquals( shipCommand,
                new ShipCommand.NewEntries( -1, -1, new RaftLogEntry[]{new RaftLogEntry( 0, CONTENT )} ) );
    }

    @Test
    public void leaderShouldHandleBatch() throws Exception
    {
        // given
        RaftState state = builder()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        final int BATCH_SIZE = 3;
        RaftMessages.NewEntry.BatchRequest batchRequest = new RaftMessages.NewEntry.BatchRequest(
                asList( valueOf( 0 ), valueOf( 1 ), valueOf( 2 ) ) );

        // when
        Outcome outcome = leader.handle( batchRequest, state, log() );

        // then
        BatchAppendLogEntries logCommand = (BatchAppendLogEntries) single( outcome.getLogCommands() );

        assertEquals( 0, logCommand.baseIndex );
        for ( int i = 0; i < BATCH_SIZE; i++ )
        {
            assertEquals( 0, logCommand.entries[i].term() );
            assertEquals( i, ((ReplicatedInteger) logCommand.entries[i].content()).get() );
        }

        ShipCommand.NewEntries shipCommand = (ShipCommand.NewEntries) single( outcome.getShipCommands() );

        assertEquals( shipCommand, new ShipCommand.NewEntries( -1, -1, new RaftLogEntry[]{
                new RaftLogEntry( 0, valueOf( 0 ) ),
                new RaftLogEntry( 0, valueOf( 1 ) ),
                new RaftLogEntry( 0, valueOf( 2 ) )
        } ) );
    }

    @Test
    public void leaderShouldCommitOnMajorityResponse() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "lalalala" ) ) );

        RaftState state = builder()
                .votingMembers( member1, member2 )
                .addInitialOutcome( OutcomeTestBuilder.builder().setLeader( myself ).build() )
                .entryLog( raftLog )
                .messagesSentToFollower( member1, raftLog.appendIndex() + 1 )
                .messagesSentToFollower( member2, raftLog.appendIndex() + 1 )
                .build();

        Leader leader = new Leader();

        // when a single instance responds (plus self == 2 out of 3 instances)
        Outcome outcome =
                leader.handle( new RaftMessages.AppendEntries.Response( member1, 0, true, 0, 0 ), state, log() );

        // then
        assertEquals( 0L, outcome.getCommitIndex() );
        assertEquals( 0L, outcome.getLeaderCommit() );
    }

    // TODO move this someplace else, since log no longer holds the commit
    @Test
    public void leaderShouldCommitAllPreviouslyAppendedEntriesWhenCommittingLaterEntryInSameTerm() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "first!" ) ) );
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "second" ) ) );
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "third" ) ) );

        RaftState state = builder()
                .votingMembers( myself, member1, member2 )
                .entryLog( raftLog )
                .messagesSentToFollower( member1, raftLog.appendIndex() + 1 )
                .messagesSentToFollower( member2, raftLog.appendIndex() + 1 )
                .build();

        Leader leader = new Leader();

        // when
        Outcome outcome = leader.handle( new AppendEntries.Response( member1, 0, true, 2, 2 ), state, log() );

        state.update( outcome );

        // then
        assertEquals( 2, state.commitIndex() );
    }

    @Test
    public void shouldSendNegativeResponseForVoteRequestFromTermNotGreaterThanLeader() throws Exception
    {
        // given
        long leaderTerm = 5;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm - 1;

        Leader leader = new Leader();
        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( leaderTerm ).setCommitIndex( leaderCommitIndex ).build() )
                .build();

        // when
        Outcome outcome = leader.handle( new RaftMessages.Vote.Request( member1, rivalTerm, member1, leaderCommitIndex, leaderTerm ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( LEADER );
        assertThat( outcome.getTerm() ).isEqualTo( leaderTerm );

        RaftMessages.RaftMessage response = messageFor( outcome, member1 );
        assertThat( response ).isInstanceOf( RaftMessages.Vote.Response.class );
        RaftMessages.Vote.Response typedResponse = (RaftMessages.Vote.Response) response;
        assertThat( typedResponse.voteGranted() ).isFalse();
    }

    @Test
    public void shouldStepDownIfReceiveVoteRequestFromGreaterTermThanLeader() throws Exception
    {
        // given
        long leaderTerm = 1;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm + 1;

        Leader leader = new Leader();
        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( leaderTerm ).setCommitIndex( leaderCommitIndex ).build() )
                .build();

        // when
        Outcome outcome = leader.handle( new RaftMessages.Vote.Request( member1, rivalTerm, member1, leaderCommitIndex, leaderTerm ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome.getLeader() ).isNull();
        assertThat( outcome.getTerm() ).isEqualTo( rivalTerm );

        RaftMessages.RaftMessage response = messageFor( outcome, member1 );
        assertThat( response ).isInstanceOf( RaftMessages.Vote.Response.class );
        RaftMessages.Vote.Response typedResponse = (RaftMessages.Vote.Response) response;
        assertThat( typedResponse.voteGranted() ).isTrue();
    }

    @Test
    public void shouldIgnoreHeartbeatFromOlderTerm() throws Exception
    {
        // given
        long leaderTerm = 5;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm - 1;

        Leader leader = new Leader();
        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( leaderTerm ).setCommitIndex( leaderCommitIndex ).build() )
                .build();

        // when
        Outcome outcome = leader.handle( new RaftMessages.Heartbeat( member1, rivalTerm, leaderCommitIndex, leaderTerm ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( LEADER );
        assertThat( outcome.getTerm() ).isEqualTo( leaderTerm );
    }

    @Test
    public void shouldStepDownIfHeartbeatReceivedWithGreaterOrEqualTerm() throws Exception
    {
        // given
        long leaderTerm = 1;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm + 1;

        Leader leader = new Leader();
        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( leaderTerm ).setCommitIndex( leaderCommitIndex ).build() )
                .build();

        // when
        Outcome outcome = leader.handle( new RaftMessages.Heartbeat( member1, rivalTerm, leaderCommitIndex, leaderTerm ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome.getLeader() ).isEqualTo( member1 );
        assertThat( outcome.getTerm() ).isEqualTo( rivalTerm );
    }

    @Test
    public void shouldRespondNegativelyToAppendEntriesRequestFromEarlierTerm() throws Exception
    {
        // given
        long leaderTerm = 5;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm - 1;
        long logIndex = 20;
        RaftLogEntry[] entries = {new RaftLogEntry( rivalTerm, ReplicatedInteger.valueOf( 99 ) )};

        Leader leader = new Leader();
        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( leaderTerm ).setCommitIndex( leaderCommitIndex ).build() )
                .build();

        // when
        Outcome outcome =
                leader.handle( new RaftMessages.AppendEntries.Request( member1, rivalTerm, logIndex, leaderTerm, entries, leaderCommitIndex ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( LEADER );
        assertThat( outcome.getTerm() ).isEqualTo( leaderTerm );

        RaftMessages.RaftMessage response = messageFor( outcome, member1 );
        assertThat( response ).isInstanceOf( RaftMessages.AppendEntries.Response.class );
        RaftMessages.AppendEntries.Response typedResponse = (RaftMessages.AppendEntries.Response) response;
        assertThat( typedResponse.term() ).isEqualTo( leaderTerm );
        assertThat( typedResponse.success() ).isFalse();
    }

    @Test
    public void shouldStepDownIfAppendEntriesRequestFromLaterTerm() throws Exception
    {
        // given
        long leaderTerm = 1;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm + 1;
        long logIndex = 20;
        RaftLogEntry[] entries = {new RaftLogEntry( rivalTerm, ReplicatedInteger.valueOf( 99 ) )};

        Leader leader = new Leader();
        RaftState state = builder()
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( leaderTerm ).setCommitIndex( leaderCommitIndex ).build() )
                .build();

        // when
        Outcome outcome =
                leader.handle( new RaftMessages.AppendEntries.Request( member1, rivalTerm, logIndex, leaderTerm, entries, leaderCommitIndex ), state, log() );

        // then
        assertThat( outcome.getRole() ).isEqualTo( FOLLOWER );
        assertThat( outcome.getLeader() ).isEqualTo( member1 );
        assertThat( outcome.getTerm() ).isEqualTo( rivalTerm );

        RaftMessages.RaftMessage response = messageFor( outcome, member1 );
        assertThat( response ).isInstanceOf( RaftMessages.AppendEntries.Response.class );
        RaftMessages.AppendEntries.Response typedResponse = (RaftMessages.AppendEntries.Response) response;
        assertThat( typedResponse.term() ).isEqualTo( rivalTerm );
        // Not checking success or failure of append
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }
}
