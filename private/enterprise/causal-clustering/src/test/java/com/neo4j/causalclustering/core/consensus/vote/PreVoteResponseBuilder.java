/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;

public class PreVoteResponseBuilder
{
    private MemberId from;
    private long term;
    private boolean voteGranted;

    public RaftMessages.PreVote.Response build()
    {
        return new RaftMessages.PreVote.Response( from, term, voteGranted );
    }

    public PreVoteResponseBuilder from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public PreVoteResponseBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public PreVoteResponseBuilder grant()
    {
        this.voteGranted = true;
        return this;
    }

    public PreVoteResponseBuilder deny()
    {
        this.voteGranted = false;
        return this;
    }
}
