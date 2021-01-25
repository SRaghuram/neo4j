/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.identity.RaftMemberId;

import java.time.Duration;

class LeaderProvider
{
    private final long timeoutMillis;
    private volatile RaftMemberId currentLeader;

    LeaderProvider( Duration leaderAwaitTimeout )
    {
        this.timeoutMillis = leaderAwaitTimeout.toMillis();
    }

    RaftMemberId awaitLeader() throws InterruptedException
    {
        RaftMemberId leader = currentLeader;
        if ( leader != null )
        {
            // fast path!
            return leader;
        }

        leader = waitForLeader();
        return leader;
    }

    private synchronized RaftMemberId waitForLeader() throws InterruptedException
    {
        if ( currentLeader == null )
        {
            wait( timeoutMillis );
        }
        return currentLeader;
    }

    synchronized void setLeader( RaftMemberId leader )
    {
        currentLeader = leader;
        if ( leader != null )
        {
            notifyAll();
        }
    }

    RaftMemberId currentLeader()
    {
        return currentLeader;
    }
}
