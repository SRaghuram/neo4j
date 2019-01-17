/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.MemberId;

public class HeartbeatBuilder
{
    private long commitIndex = -1;
    private long leaderTerm = -1;
    private long commitIndexTerm = -1;
    private MemberId from;

    public RaftMessages.Heartbeat build()
    {
        return new RaftMessages.Heartbeat( from, leaderTerm, commitIndex, commitIndexTerm );
    }

    public HeartbeatBuilder from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public HeartbeatBuilder leaderTerm( long leaderTerm )
    {
        this.leaderTerm = leaderTerm;
        return this;
    }

    public HeartbeatBuilder commitIndex( long commitIndex )
    {
        this.commitIndex = commitIndex;
        return this;
    }

    public HeartbeatBuilder commitIndexTerm( long commitIndexTerm )
    {
        this.commitIndexTerm = commitIndexTerm;
        return this;
    }
}
