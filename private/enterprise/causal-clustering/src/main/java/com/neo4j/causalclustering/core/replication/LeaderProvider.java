/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.identity.MemberId;

import java.time.Duration;

class LeaderProvider
{
    private final long timeoutMillis;
    private volatile MemberId currentLeader;

    LeaderProvider( Duration leaderAwaitTimeout )
    {
        this.timeoutMillis = leaderAwaitTimeout.toMillis();
    }

    MemberId awaitLeader() throws InterruptedException
    {
        MemberId leader = currentLeader;
        if ( leader != null )
        {
            // fast path!
            return leader;
        }

        leader = waitForLeader();
        return leader;
    }

    private synchronized MemberId waitForLeader() throws InterruptedException
    {
        if ( currentLeader == null )
        {
            wait( timeoutMillis );
        }
        return currentLeader;
    }

    synchronized void setLeader( MemberId leader )
    {
        currentLeader = leader;
        if ( leader != null )
        {
            notifyAll();
        }
    }

    MemberId currentLeader()
    {
        return currentLeader;
    }
}
