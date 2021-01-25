/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;

import java.time.Clock;

class CatchupGoal
{
    private static final long MAX_ROUNDS = 10;

    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final long electionTimeout;

    private long targetIndex;
    private long roundCount;
    private long startTime;

    CatchupGoal( ReadableRaftLog raftLog, Clock clock, long electionTimeout )
    {
        this.raftLog = raftLog;
        this.clock = clock;
        this.electionTimeout = electionTimeout;
        this.targetIndex = raftLog.appendIndex();
        this.startTime = clock.millis();

        this.roundCount = 1;
    }

    boolean achieved( FollowerState followerState )
    {
        if ( followerState.getMatchIndex() >= targetIndex )
        {
            if ( (clock.millis() - startTime) <= electionTimeout )
            {
                return true;
            }
            else if ( roundCount <  MAX_ROUNDS )
            {
                roundCount++;
                startTime = clock.millis();
                targetIndex = raftLog.appendIndex();
            }
        }
        return false;
    }
}
