/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static com.neo4j.causalclustering.core.consensus.roles.AppendEntriesRequestTest.ContentGenerator.content;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class AppendEntriesRequestTest
{

    @Parameterized.Parameters( name = "{0} with leader {1} terms ahead." )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {Role.FOLLOWER, 0}, {Role.FOLLOWER, 1}, {Role.LEADER, 1}, {Role.CANDIDATE, 1}
        } );
    }

    @Parameterized.Parameter()
    public Role role;

    @Parameterized.Parameter( value = 1 )
    public int leaderTermDifference;

    private MemberId myself = member( 0 );
    private MemberId leader = member( 1 );

    @Test
    public void shouldAcceptInitialEntryAfterBootstrap() throws Exception
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
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands() ).contains( new BatchAppendLogEntries( 1, 0, new RaftLogEntry[]{logEntry} ) );
    }

    @Test
    public void shouldAcceptInitialEntriesAfterBootstrap() throws Exception
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
                .build(), state, log() );

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

    @Test
    public void shouldRejectDiscontinuousEntries() throws Exception
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
                .build(), state, log() );

        // then
        Response response = (Response) messageFor( outcome, leader );
        assertEquals( state.entryLog().appendIndex(), response.appendIndex() );
        assertFalse( response.success() );
    }

    @Test
    public void shouldAcceptContinuousEntries() throws Exception
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
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
    }

    @Test
    public void shouldTruncateOnReceiptOfConflictingEntry() throws Exception
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
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands() ).contains( new TruncateLogCommand( 1 ) );
    }

    @Test
    public void shouldCommitEntry() throws Exception
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
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getCommitIndex() ).isEqualTo( 0 );
    }

    @Test
    public void shouldAppendNewEntryAndCommitPreviouslyAppendedEntry() throws Exception
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
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getCommitIndex() ).isEqualTo( 0 );
        assertThat( outcome.getLogCommands() ).contains( new BatchAppendLogEntries( 1, 0,
                new RaftLogEntry[]{newLogEntry} ) );
    }

    @Test
    public void shouldNotCommitAheadOfMatchingHistory() throws Exception
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
                .build(), state, log() );

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
