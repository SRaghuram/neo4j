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

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class CatchupGoalTest
{
    @Test
    void goalAchievedWhenCatchupRoundDurationLessThanTarget()
    {
        FakeClock clock = Clocks.fakeClock();
        long electionTimeout = 15;
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoal goal = new CatchupGoal( log, clock, electionTimeout );

        log.setAppendIndex( 20 );
        clock.forward( 10, MILLISECONDS );
        Assertions.assertFalse( goal.achieved( new FollowerState() ) );

        log.setAppendIndex( 30 );
        clock.forward( 10, MILLISECONDS );
        Assertions.assertFalse( goal.achieved( new FollowerState().onSuccessResponse( 10 ) ) );

        log.setAppendIndex( 40 );
        clock.forward( 10, MILLISECONDS );
        Assertions.assertTrue( goal.achieved( new FollowerState().onSuccessResponse( 30 ) ) );
    }

    private static class StubLog implements ReadableRaftLog
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
