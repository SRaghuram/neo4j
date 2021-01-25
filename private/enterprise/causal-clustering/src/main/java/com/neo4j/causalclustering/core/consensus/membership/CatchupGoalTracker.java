/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;

import java.time.Clock;

class CatchupGoalTracker
{
    static final long MAX_ROUNDS = 10;

    private final ReadableRaftLog raftLog;
    private final Clock clock;

    private long startTime;
    private  long roundStartTime;
    private final long roundTimeout;
    private long roundCount;
    private long catchupTimeout;

    private long targetIndex;
    private boolean finished;
    private boolean goalAchieved;

    CatchupGoalTracker( ReadableRaftLog raftLog, Clock clock, long roundTimeout, long catchupTimeout )
    {
        this.raftLog = raftLog;
        this.clock = clock;
        this.roundTimeout = roundTimeout;
        this.catchupTimeout = catchupTimeout;
        this.targetIndex = raftLog.appendIndex();
        this.startTime = clock.millis();
        this.roundStartTime = clock.millis();

        this.roundCount = 1;
    }

    void updateProgress( FollowerState followerState )
    {
        if ( finished )
        {
            return;
        }

        boolean achievedTarget = followerState.getMatchIndex() >= targetIndex;
        if ( achievedTarget && (clock.millis() - roundStartTime) <= roundTimeout )
        {
            goalAchieved = true;
            finished = true;
        }
        else if ( clock.millis() > (startTime + catchupTimeout) )
        {
            finished = true;
        }
        else if ( achievedTarget )
        {
            if ( roundCount < MAX_ROUNDS )
            {
                roundCount++;
                roundStartTime = clock.millis();
                targetIndex = raftLog.appendIndex();
            }
            else
            {
                finished = true;
            }
        }
    }

    boolean isFinished()
    {
        return finished;
    }

    boolean isGoalAchieved()
    {
        return goalAchieved;
    }

    @Override
    public String toString()
    {
        return String.format( "CatchupGoalTracker{startTime=%d, roundStartTime=%d, roundTimeout=%d, roundCount=%d, " +
                "catchupTimeout=%d, targetIndex=%d, finished=%s, goalAchieved=%s}", startTime, roundStartTime,
                roundTimeout, roundCount, catchupTimeout, targetIndex, finished, goalAchieved );
    }
}
