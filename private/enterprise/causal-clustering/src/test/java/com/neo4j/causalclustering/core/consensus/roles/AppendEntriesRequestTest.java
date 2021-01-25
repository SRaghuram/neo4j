/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries.Response;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.BatchAppendLogEntries;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static com.neo4j.causalclustering.core.consensus.roles.AppendEntriesRequestTest.ContentGenerator.content;
import static com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContextBuilder.contextWithState;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AppendEntriesRequestTest
{

    public static Stream<Object[]> data()
    {
        return Stream.of( new Object[][]{
                {Role.FOLLOWER, 0}, {Role.FOLLOWER, 1}, {Role.LEADER, 1}, {Role.CANDIDATE, 1}
        } );
    }

    private RaftMemberId myself = raftMember( 0 );
    private RaftMemberId leader = raftMember( 1 );

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldAcceptInitialEntryAfterBootstrap( Role role, int leaderTermDifference ) throws Exception
    {
        RaftLog raftLog = bootstrappedLog();
        RaftState state = builder()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry logEntry = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( 0 )
                .prevLogTerm( 0 )
                .logEntry( logEntry )
                .build(), contextWithState( state ), log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands() ).contains( new BatchAppendLogEntries( 1, 0, new RaftLogEntry[]{logEntry} ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldAcceptInitialEntriesAfterBootstrap( Role role, int leaderTermDifference ) throws Exception
    {
        RaftLog raftLog = bootstrappedLog();
        RaftState state = builder()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry logEntry1 = new RaftLogEntry( leaderTerm, content() );
        RaftLogEntry logEntry2 = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( 0 )
                .prevLogTerm( 0 )
                .logEntry( logEntry1 )
                .logEntry( logEntry2 )
                .build(), contextWithState( state ), log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands() ).contains( new BatchAppendLogEntries( 1, 0, new RaftLogEntry[]{logEntry1, logEntry2} ) );
    }

    private RaftLog bootstrappedLog()
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, content() ) );
        return raftLog;
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldRejectDiscontinuousEntries( Role role, int leaderTermDifference ) throws Exception
    {
        // given
        RaftState state = builder()
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( state.entryLog().appendIndex() + 1 )
                .prevLogTerm( leaderTerm )
                .logEntry( new RaftLogEntry( leaderTerm, content() ) )
                .build(), contextWithState( state ), log() );

        // then
        Response response = (Response) messageFor( outcome, leader );
        assertEquals( state.entryLog().appendIndex(), response.appendIndex() );
        assertFalse( response.success() );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldAcceptContinuousEntries( Role role, int leaderTermDifference ) throws Exception
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() )
                .prevLogTerm( leaderTerm )
                .logEntry( new RaftLogEntry( leaderTerm, content() ) )
                .build(), contextWithState( state ), log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldTruncateOnReceiptOfConflictingEntry( Role role, int leaderTermDifference ) throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .myself( myself )
                .addInitialOutcome( OutcomeTestBuilder.builder().setTerm( 5 ).build() )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( state.term() - 1, content() ) );
        raftLog.append( new RaftLogEntry( state.term() - 1, content() ) );

        // when
        long previousIndex = raftLog.appendIndex() - 1;
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( previousIndex )
                .prevLogTerm( raftLog.readEntryTerm( previousIndex ) )
                .logEntry( new RaftLogEntry( leaderTerm, content() ) )
                .build(), contextWithState( state ), log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands() ).contains( new TruncateLogCommand( 1 ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldCommitEntry( Role role, int leaderTermDifference ) throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() )
                .prevLogTerm( leaderTerm )
                .leaderCommit( 0 )
                .build(), contextWithState( state ), log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getCommitIndex() ).isEqualTo( 0 );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldAppendNewEntryAndCommitPreviouslyAppendedEntry( Role role, int leaderTermDifference ) throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry previouslyAppendedEntry = new RaftLogEntry( leaderTerm, content() );
        raftLog.append( previouslyAppendedEntry );
        RaftLogEntry newLogEntry = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() )
                .prevLogTerm( leaderTerm )
                .logEntry( newLogEntry )
                .leaderCommit( 0 )
                .build(), contextWithState( state ), log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getCommitIndex() ).isEqualTo( 0 );
        assertThat( outcome.getLogCommands() ).contains( new BatchAppendLogEntries( 1, 0,
                new RaftLogEntry[]{newLogEntry} ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldNotCommitAheadOfMatchingHistory( Role role, int leaderTermDifference ) throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry previouslyAppendedEntry = new RaftLogEntry( leaderTerm, content() );
        raftLog.append( previouslyAppendedEntry );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() + 1 )
                .prevLogTerm( leaderTerm )
                .leaderCommit( 0 )
                .build(), contextWithState( state ), log() );

        // then
        assertFalse( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands() ).isEmpty();
    }

    public RaftState newState() throws IOException
    {
        return builder().myself( myself ).build();
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

    static class ContentGenerator
    {
        private static int count;

        public static ReplicatedString content()
        {
            return new ReplicatedString( String.format( "content#%d", count++ ) );
        }
    }
}
