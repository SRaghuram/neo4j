/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;

public class VoteRequestBuilder
{
    private long lastLogTerm;
    private MemberId from;
    private long term;
    private MemberId candidate;
    private long lastLogIndex;

    public VoteRequestBuilder from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public VoteRequestBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public VoteRequestBuilder candidate( MemberId candidate )
    {
        this.candidate = candidate;
        return this;
    }

    public VoteRequestBuilder lastLogIndex( long lastLogIndex )
    {
        this.lastLogIndex = lastLogIndex;
        return this;
    }

    public VoteRequestBuilder lastLogTerm( long lastLogTerm )
    {
        this.lastLogTerm = lastLogTerm;
        return this;
    }

    public RaftMessages.Vote.Request build()
    {
        return new RaftMessages.Vote.Request( from, term, candidate, lastLogIndex, lastLogTerm );
    }
}
