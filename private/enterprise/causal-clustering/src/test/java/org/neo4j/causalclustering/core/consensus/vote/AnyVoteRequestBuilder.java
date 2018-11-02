/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.vote;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.identity.MemberId;

public abstract class AnyVoteRequestBuilder<T extends RaftMessages.AnyVote.Request>
{
    protected AnyVoteRequestBuilder( Constructor<T> constructor )
    {
        this.constructor = constructor;
    }

    @FunctionalInterface
    interface Constructor<T extends RaftMessages.AnyVote.Request>
    {
        T construct( MemberId from, long term, MemberId candidate, long lastLogIndex, long lastLogTerm );
    }

    private long term = -1;
    private MemberId from;
    private MemberId candidate;
    private long lastLogIndex;
    private long lastLogTerm;

    private final Constructor<T> constructor;

    public T build()
    {
        return constructor.construct( from, term, candidate, lastLogIndex, lastLogTerm );
    }

    public AnyVoteRequestBuilder<T> from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public AnyVoteRequestBuilder<T> term( long term )
    {
        this.term = term;
        return this;
    }

    public AnyVoteRequestBuilder<T> candidate( MemberId candidate )
    {
        this.candidate = candidate;
        return this;
    }

    public AnyVoteRequestBuilder<T> lastLogIndex( long lastLogIndex )
    {
        this.lastLogIndex = lastLogIndex;
        return this;
    }

    public AnyVoteRequestBuilder<T> lastLogTerm( long lastLogTerm )
    {
        this.lastLogTerm = lastLogTerm;
        return this;
    }
}
