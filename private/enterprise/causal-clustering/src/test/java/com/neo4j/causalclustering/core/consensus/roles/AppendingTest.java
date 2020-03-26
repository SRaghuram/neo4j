/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import com.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.MemberId;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AppendingTest
{
    private MemberId aMember = member( 0 );

    @Test
    void shouldPerformTruncation() throws Exception
    {
        // when
        // we have a log appended up to appendIndex, and committed somewhere before that
        long appendIndex = 5;
        long localTermForAllEntries = 1L;

        var outcomeBuilder = OutcomeTestBuilder.builder();
        var logMock = mock( ReadableRaftLog.class );
        when( logMock.readEntryTerm( anyLong() ) ).thenReturn( localTermForAllEntries ); // for simplicity, all entries are at term 1
        when( logMock.appendIndex() ).thenReturn( appendIndex );

        ReadableRaftState state = mock( ReadableRaftState.class );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.commitIndex() ).thenReturn( appendIndex - 3 );

        // when
        // the leader asks to append after the commit index an entry that mismatches on term
        Appending.handleAppendEntriesRequest( state, outcomeBuilder,
                new RaftMessages.AppendEntries.Request( aMember, localTermForAllEntries, appendIndex - 2,
                        localTermForAllEntries,
                        new RaftLogEntry[]{
                                new RaftLogEntry( localTermForAllEntries + 1, ReplicatedInteger.valueOf( 2 ) )},
                        appendIndex + 3 ) );

        // then
        // we must produce a TruncateLogCommand at the earliest mismatching index
        var outcome = outcomeBuilder.build();
        assertThat( outcome.getLogCommands() ).contains( new TruncateLogCommand( appendIndex - 1 ) );
    }

    @Test
    void shouldNotAllowTruncationAtCommit() throws Exception
    {
        // given
        long commitIndex = 5;
        long localTermForAllEntries = 1L;

        var outcomeBuilder = OutcomeTestBuilder.builder();
        var logMock = mock( ReadableRaftLog.class );
        when( logMock.readEntryTerm( anyLong() ) ).thenReturn( localTermForAllEntries ); // for simplicity, all entries are at term 1
        when( logMock.appendIndex() ).thenReturn( commitIndex );

        ReadableRaftState state = mock( ReadableRaftState.class );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.commitIndex() ).thenReturn( commitIndex );

        // when - then
        try
        {
            Appending.handleAppendEntriesRequest( state, outcomeBuilder,
                    new RaftMessages.AppendEntries.Request( aMember, localTermForAllEntries, commitIndex - 1,
                            localTermForAllEntries,
                            new RaftLogEntry[]{
                                    new RaftLogEntry( localTermForAllEntries + 1, ReplicatedInteger.valueOf( 2 ) )},
                            commitIndex + 3 ) );
            Assertions.fail( "Appending should not allow truncation at or before the commit index" );
        }
        catch ( IllegalStateException expected )
        {
            // ok
        }
    }

    @Test
    void shouldNotAllowTruncationBeforeCommit() throws Exception
    {
        // given
        long commitIndex = 5;
        long localTermForAllEntries = 1L;

        var outcomeBuilder = OutcomeTestBuilder.builder();
        var logMock = mock( ReadableRaftLog.class );
        when( logMock.readEntryTerm( anyLong() ) ).thenReturn( localTermForAllEntries ); // for simplicity, all entries are at term 1
        when( logMock.appendIndex() ).thenReturn( commitIndex );

        var state = mock( ReadableRaftState.class );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.commitIndex() ).thenReturn( commitIndex );

        // when - then
        try
        {
            Appending.handleAppendEntriesRequest( state, outcomeBuilder,
                    new RaftMessages.AppendEntries.Request( aMember, localTermForAllEntries, commitIndex - 2,
                            localTermForAllEntries,
                            new RaftLogEntry[]{
                                    new RaftLogEntry( localTermForAllEntries + 1, ReplicatedInteger.valueOf( 2 ) )},
                            commitIndex + 3 ) );
            Assertions.fail( "Appending should not allow truncation at or before the commit index" );
        }
        catch ( IllegalStateException expected )
        {
            // fine
        }
    }

    @Test
    void shouldNotAttemptToTruncateAtIndexBeforeTheLogPrevIndex() throws Exception
    {
        // given
        // a log with prevIndex and prevTerm set
        var logMock = mock( ReadableRaftLog.class );
        long prevIndex = 5;
        long prevTerm = 5;
        when( logMock.prevIndex() ).thenReturn( prevIndex );
        when( logMock.readEntryTerm( prevIndex ) ).thenReturn( prevTerm );
        // and which also properly returns -1 as the term for an unknown entry, in this case prevIndex - 2
        when( logMock.readEntryTerm( prevIndex - 2 ) ).thenReturn( -1L );

        // also, a state with a given commitIndex, obviously ahead of prevIndex
        long commitIndex = 10;
        var state = mock( ReadableRaftState.class );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.commitIndex() ).thenReturn( commitIndex );
        // which is also the append index
        when( logMock.appendIndex() ).thenReturn( commitIndex );

        // when
        // an appendEntriesRequest arrives for appending entries before the prevIndex (for whatever reason)
        var outcomeBuilder = OutcomeTestBuilder.builder();
        Appending.handleAppendEntriesRequest( state, outcomeBuilder,
                new RaftMessages.AppendEntries.Request( aMember, prevTerm, prevIndex - 2,
                        prevTerm,
                        new RaftLogEntry[]{
                                new RaftLogEntry( prevTerm, ReplicatedInteger.valueOf( 2 ) )},
                        commitIndex + 3 ) );

        // then
        // there should be no truncate commands. Actually, the whole thing should be a no op
        var outcome = outcomeBuilder.build();
        assertThat( outcome.getLogCommands() ).isEmpty();
    }

    private static class LogCommandMatcher extends TypeSafeMatcher<RaftLogCommand>
    {
        private final long truncateIndex;

        private LogCommandMatcher( long truncateIndex )
        {
            this.truncateIndex = truncateIndex;
        }

        @Override
        protected boolean matchesSafely( RaftLogCommand item )
        {
            return item instanceof TruncateLogCommand && ((TruncateLogCommand) item).fromIndex == truncateIndex;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( new TruncateLogCommand( truncateIndex ).toString() );
        }
    }
}
