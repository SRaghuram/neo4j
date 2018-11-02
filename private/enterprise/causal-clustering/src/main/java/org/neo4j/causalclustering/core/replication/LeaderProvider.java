/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

import org.neo4j.causalclustering.identity.MemberId;

class LeaderProvider
{
    private MemberId currentLeader;

    synchronized MemberId awaitLeader() throws InterruptedException
    {
        while ( currentLeader == null )
        {
            wait();
        }
        return currentLeader;
    }

    synchronized void setLeader( MemberId leader )
    {
        this.currentLeader = leader;
        if ( currentLeader != null )
        {
            notifyAll();
        }
    }

    MemberId currentLeader()
    {
        return currentLeader;
    }
}
