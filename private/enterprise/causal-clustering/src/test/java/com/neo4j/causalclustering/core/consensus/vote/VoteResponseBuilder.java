/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftMemberId;

public class VoteResponseBuilder
{
    private RaftMemberId from;
    private long term;
    private boolean voteGranted;

    public RaftMessages.Vote.Response build()
    {
        return new RaftMessages.Vote.Response( from, term, voteGranted );
    }

    public VoteResponseBuilder from( RaftMemberId from )
    {
        this.from = from;
        return this;
    }

    public VoteResponseBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public VoteResponseBuilder grant()
    {
        this.voteGranted = true;
        return this;
    }

    public VoteResponseBuilder deny()
    {
        this.voteGranted = false;
        return this;
    }
}
