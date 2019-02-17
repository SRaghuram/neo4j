/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.core.consensus.log.RaftLogHelper.hasNoContent;
import static com.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class RaftLogContractTest
{
    public abstract RaftLog createRaftLog();

    @Test
    void shouldReportCorrectDefaultValuesOnEmptyLog() throws Exception
    {
        // given
        ReadableRaftLog log = createRaftLog();

        // then
        assertThat( log.appendIndex(), is( -1L ) );
        assertThat( log.prevIndex(), is( -1L ) );
        assertThat( log.readEntryTerm( 0 ), is( -1L ) );
        assertThat( log.readEntryTerm( -1 ), is( -1L ) );
    }

    @Test
    void shouldResetHighTermOnTruncate() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append(
                new RaftLogEntry( 45, valueOf( 99 ) ),
                new RaftLogEntry( 46, valueOf( 99 ) ),
                new RaftLogEntry( 47, valueOf( 99 ) ) );

        // truncate the last 2
        log.truncate( 1 );

        // then
        log.append( new RaftLogEntry( 46, valueOf( 9999 ) ) );

        assertThat( log.readEntryTerm( 1 ), is( 46L ) );
        assertThat( log.appendIndex(), is( 1L ) );
    }

    @Test
    void shouldAppendDataAndNotCommitImmediately() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntry = new RaftLogEntry( 1, valueOf( 1 ) );
        log.append( logEntry );

        assertThat( log.appendIndex(), is( 0L ) );
        assertThat( readLogEntry( log, 0 ), equalTo( logEntry ) );
    }

    @Test
    void shouldTruncatePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, valueOf( 2 ) );

        log.append( logEntryA, logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );

        log.truncate( 1 );

        assertThat( log.appendIndex(), is( 0L ) );
    }

    @Test
    void shouldReplacePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, valueOf( 2 ) );
        RaftLogEntry logEntryC = new RaftLogEntry( 1, valueOf( 3 ) );
        RaftLogEntry logEntryD = new RaftLogEntry( 1, valueOf( 4 ) );
        RaftLogEntry logEntryE = new RaftLogEntry( 1, valueOf( 5 ) );

        log.append( logEntryA, logEntryB, logEntryC );

        log.truncate( 1 );

        log.append( logEntryD, logEntryE );

        assertThat( log.appendIndex(), is( 2L ) );
        assertThat( readLogEntry( log, 0 ), equalTo( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), equalTo( logEntryD ) );
        assertThat( readLogEntry( log, 2 ), equalTo( logEntryE ) );
    }

    @Test
    void shouldHaveNoEffectWhenTruncatingNonExistingEntries() throws Exception
    {
        // Given
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, valueOf( 2 ) );

        log.append( logEntryA, logEntryB );

        assertThrows( IllegalArgumentException.class, () -> log.truncate( 5 ) );

        // Then, assert that the state is unchanged
        assertThat( log.appendIndex(), is( 1L ) );
        assertThat( readLogEntry( log, 0 ), equalTo( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), equalTo( logEntryB ) );
    }

    @Test
    void shouldLogDifferentContentTypes() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedString.valueOf( "hejzxcjkzhxcjkxz" ) );

        log.append( logEntryA, logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );

        assertThat( readLogEntry( log, 0 ), equalTo( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), equalTo( logEntryB ) );
    }

    @Test
    void shouldRejectNonMonotonicTermsForEntries() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append(
                new RaftLogEntry( 0, valueOf( 1 ) ),
                new RaftLogEntry( 1, valueOf( 2 ) ) );

        assertThrows( IllegalStateException.class, () -> log.append( new RaftLogEntry( 0, valueOf( 3 ) ) ) );
    }

    @Test
    void shouldAppendAndThenTruncateSubsequentEntry() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        long toBeSpared = log.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        long toTruncate = log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        // when
        log.truncate( toTruncate );

        // then
        assertThat( log.appendIndex(), is( toBeSpared ) );
        assertThat( log.readEntryTerm( toBeSpared ), is( 0L ) );
    }

    @Test
    void shouldAppendAfterTruncating() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        long toCommit = log.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        long toTruncate = log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        // when
        log.truncate( toTruncate );
        long lastAppended = log.append( new RaftLogEntry( 2, valueOf( 3 ) ) );

        // then
        assertThat( log.appendIndex(), is( lastAppended ) );
        assertThat( log.readEntryTerm( toCommit ), is( 0L ) );
        assertThat( log.readEntryTerm( lastAppended ), is( 2L ) );
    }

    @Test
    void shouldEventuallyPrune() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        int term = 0;

        long safeIndex = -1;
        long prunedIndex = -1;

        // this loop should eventually be able to prune something
        while ( prunedIndex == -1 )
        {
            for ( int i = 0; i < 100; i++ )
            {
                log.append( new RaftLogEntry( term, valueOf( 10 * term ) ) );
                term++;
            }
            safeIndex = log.appendIndex() - 50;
            // when
            prunedIndex = log.prune( safeIndex );
        }

        // then
        assertThat( prunedIndex, lessThanOrEqualTo( safeIndex ) );
        assertEquals( prunedIndex, log.prevIndex() );
        assertEquals( prunedIndex, log.readEntryTerm( prunedIndex ) );

        final long[] expectedVal = {prunedIndex + 1};
        log.getEntryCursor( prunedIndex + 1 )
                .forAll( entry -> assertThat( entry.content(), is( valueOf( 10 * (int)expectedVal[0]++ ) ) ) );

        assertThat( log, hasNoContent( prunedIndex ) );
    }

    @Test
    void shouldSkipAheadInEmptyLog() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        // when
        long skipIndex = 10;
        long skipTerm = 2;
        log.skip( skipIndex, skipTerm );

        // then
        assertEquals( skipIndex, log.appendIndex() );
        assertEquals( skipTerm, log.readEntryTerm( skipIndex ) );
    }

    @Test
    void shouldSkipAheadInLogWithContent() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        long term = 0;
        int entryCount = 5;
        for ( int i = 0; i < entryCount; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }

        // when
        long skipIndex = entryCount + 5;
        long skipTerm = term + 2;
        log.skip( skipIndex, skipTerm );

        // then
        assertEquals( skipIndex, log.appendIndex() );
        assertEquals( skipTerm, log.readEntryTerm( skipIndex ) );
    }

    @Test
    void shouldNotSkipInLogWithLaterContent() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        long term = 0;
        int entryCount = 5;
        for ( int i = 0; i < entryCount; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }
        long lastIndex = log.appendIndex();

        // when
        long skipIndex = entryCount - 2;
        log.skip( skipIndex, term );

        // then
        assertEquals( lastIndex, log.appendIndex() );
        assertEquals( term, log.readEntryTerm( skipIndex ) );
    }

    @Test
    void shouldBeAbleToAppendAfterSkipping() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        // when
        long skipIndex = 5;
        long term = 0;
        log.skip( skipIndex, term );

        int newContentValue = 100;
        long newEntryIndex = skipIndex + 1;
        long appendedIndex = log.append( new RaftLogEntry( term, valueOf( newContentValue ) ) );

        // then
        assertEquals( newEntryIndex, log.appendIndex() );
        assertEquals( newEntryIndex, appendedIndex );

        assertThrows( IOException.class, () -> readLogEntry( log, skipIndex ) );

        assertThat( readLogEntry( log, newEntryIndex ).content(), is( valueOf( newContentValue ) ) );
    }

    @Test
    void pruneShouldNotChangePrevIndexAfterSkipping() throws Exception
    {
        /**
         * Given the situation where a skip happens followed by a prune, you may have the prune operation incorrectly
         * set the prevIndex to be the value of the last segment in the log, disregarding the skip command.
         * This test ensures that in this scenario, we will respect the current prevIndex value if it has been set to
         * something in the future (i.e. skip) rather than modify it to be the value of the last segment.
         *
         * Initial Scenario:    [0][1][2][3][4][5][6][7][8][9]              prevIndex = 0
         * Skip to 20 :         [0][1][2][3][4][5][6][7][8][9]...[20]               prevIndex = 20
         * Prune segment 8:                                [9]...[20]               prevIndex = 20 //not 9
         */

        // given
        RaftLog log = createRaftLog();

        long term = 0;
        for ( int i = 0; i < 2000; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }

        long skipIndex = 3000;
        log.skip( skipIndex, term );
        assertEquals( skipIndex, log.prevIndex() );

        // when
        log.prune( skipIndex );

        // then
        assertEquals( skipIndex, log.prevIndex() );
    }

    @Test
    void shouldProperlyReportExistenceOfIndexesAfterSkipping() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        long term = 0;
        long existingEntryIndex = log.append( new RaftLogEntry( term, valueOf( 100 ) ) );

        long skipIndex = 15;

        // when
        log.skip( skipIndex, term );

        // then
        assertEquals( skipIndex, log.appendIndex() );

        // all indexes starting from the next of the last appended to the skipped index (and forward) should not be present
        for ( long i = existingEntryIndex + 1; i < skipIndex + 2; i++ )
        {
            long index = i;
            assertThrows( IOException.class, () -> readLogEntry( log, index ) );
        }
    }

    @Test
    void shouldThrowExceptionWhenReadingAnEntryWhichHasBeenPruned() throws Exception
    {
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, string( 1024 ) ) );
        log.append( new RaftLogEntry( 1, string( 1024 ) ) );
        log.append( new RaftLogEntry( 2, string( 1024 ) ) );
        log.append( new RaftLogEntry( 3, string( 1024 ) ) );
        log.append( new RaftLogEntry( 4, string( 1024 ) ) );

        long pruneIndex = log.prune( 4 );
        assertThat( pruneIndex, greaterThanOrEqualTo( 2L ) );

        long term = log.readEntryTerm( 1 );

        RaftLogCursor cursor = log.getEntryCursor( 1 );
        if ( cursor.next() )
        {
            fail(); //the cursor should return false since this has been pruned.
        }

        assertEquals( -1L, term );
    }

    private ReplicatedString string( int numberOfCharacters )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < numberOfCharacters; i++ )
        {
            builder.append( String.valueOf( i ) );
        }
        return ReplicatedString.valueOf( builder.toString() );
    }

    // TODO: Test what happens when the log has rotated, *not* pruned and then skipping happens which causes
    // TODO: archived logs to be forgotten about. Does it still return the entries or at least fail gracefully?
    // TODO: In the case of PhysicalRaftLog, are the ranges kept properly up to date to notify of non existing files?
}
