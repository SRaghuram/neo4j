/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

class CatchupGoalTrackerTest
{

    private static final long ROUND_TIMEOUT = 15;
    private static final long CATCHUP_TIMEOUT = 1_000;

    @Test
    void shouldAchieveGoalIfWithinRoundTimeout()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        clock.forward( ROUND_TIMEOUT - 5, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 10 ) );

        Assertions.assertTrue( catchupGoalTracker.isGoalAchieved() );
        Assertions.assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    void shouldNotAchieveGoalIfBeyondRoundTimeout()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        clock.forward( ROUND_TIMEOUT + 5, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 10 ) );

        Assertions.assertFalse( catchupGoalTracker.isGoalAchieved() );
        Assertions.assertFalse( catchupGoalTracker.isFinished() );
    }

    @Test
    void shouldFailToAchieveGoalDueToCatchupTimeoutExpiring()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        // when
        clock.forward( CATCHUP_TIMEOUT + 10, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 4 ) );

        // then
        Assertions.assertFalse( catchupGoalTracker.isGoalAchieved() );
        Assertions.assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    void shouldFailToAchieveGoalDueToCatchupTimeoutExpiringEvenThoughWeDoEventuallyAchieveTarget()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        // when
        clock.forward( CATCHUP_TIMEOUT + 10, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 10 ) );

        // then
        Assertions.assertFalse( catchupGoalTracker.isGoalAchieved() );
        Assertions.assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    void shouldFailToAchieveGoalDueToRoundExhaustion()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        long appendIndex = 10;
        log.setAppendIndex( appendIndex );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        for ( int i = 0; i < CatchupGoalTracker.MAX_ROUNDS; i++ )
        {
            appendIndex += 10;
            log.setAppendIndex( appendIndex );
            clock.forward( ROUND_TIMEOUT + 1, TimeUnit.MILLISECONDS );
            catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( appendIndex ) );
        }

        // then
        Assertions.assertFalse( catchupGoalTracker.isGoalAchieved() );
        Assertions.assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    void shouldNotFinishIfRoundsNotExhausted()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        long appendIndex = 10;
        log.setAppendIndex( appendIndex );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        for ( int i = 0; i < CatchupGoalTracker.MAX_ROUNDS - 5; i++ )
        {
            appendIndex += 10;
            log.setAppendIndex( appendIndex );
            clock.forward( ROUND_TIMEOUT + 1, TimeUnit.MILLISECONDS );
            catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( appendIndex ) );
        }

        // then
        Assertions.assertFalse( catchupGoalTracker.isGoalAchieved() );
        Assertions.assertFalse( catchupGoalTracker.isFinished() );
    }

    private class StubLog implements ReadableRaftLog
    {
        private long appendIndex;

        private void setAppendIndex( long index )
        {
            this.appendIndex = index;
        }

        @Override
        public long appendIndex()
        {
            return appendIndex;
        }

        @Override
        public long prevIndex()
        {
            return 0;
        }

        @Override
        public long readEntryTerm( long logIndex )
        {
            return 0;
        }

        @Override
        public RaftLogCursor getEntryCursor( long fromIndex )
        {
            return RaftLogCursor.empty();
        }
    }
}
