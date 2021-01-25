/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.LinkedList;
import java.util.List;

public class AppendEntriesRequestBuilder
{
    private List<RaftLogEntry> logEntries = new LinkedList<>();
    private long leaderCommit = -1;
    private long prevLogTerm = -1;
    private long prevLogIndex = -1;
    private long leaderTerm = -1;
    private RaftMemberId from;

    public RaftMessages.AppendEntries.Request build()
    {
        return new RaftMessages.AppendEntries.Request( from, leaderTerm, prevLogIndex, prevLogTerm,
                                                       logEntries.toArray( new RaftLogEntry[0] ), leaderCommit );
    }

    public AppendEntriesRequestBuilder from( RaftMemberId from )
    {
        this.from = from;
        return this;
    }

    public AppendEntriesRequestBuilder leaderTerm( long leaderTerm )
    {
        this.leaderTerm = leaderTerm;
        return this;
    }

    public AppendEntriesRequestBuilder prevLogIndex( long prevLogIndex )
    {
        this.prevLogIndex = prevLogIndex;
        return this;
    }

    public AppendEntriesRequestBuilder prevLogTerm( long prevLogTerm )
    {
        this.prevLogTerm = prevLogTerm;
        return this;
    }

    public AppendEntriesRequestBuilder logEntry( RaftLogEntry logEntry )
    {
        logEntries.add( logEntry );
        return this;
    }

    public AppendEntriesRequestBuilder leaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
        return this;
    }
}
