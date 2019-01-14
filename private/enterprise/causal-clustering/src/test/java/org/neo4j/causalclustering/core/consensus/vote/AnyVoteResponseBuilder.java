/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.vote;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.identity.MemberId;

public abstract class AnyVoteResponseBuilder<T extends RaftMessages.AnyVote.Response>
{
    protected AnyVoteResponseBuilder( Constructor<T> constructor )
    {
        this.constructor = constructor;
    }

    @FunctionalInterface
    interface Constructor<T extends RaftMessages.AnyVote.Response>
    {
        T construct( MemberId from, long term, boolean voteGranted );
    }

    private boolean voteGranted;
    private long term = -1;
    private MemberId from;
    private final Constructor<T> constructor;

    public T build()
    {
        return constructor.construct( from, term, voteGranted );
    }

    public AnyVoteResponseBuilder<T> from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public AnyVoteResponseBuilder<T> term( long term )
    {
        this.term = term;
        return this;
    }

    public AnyVoteResponseBuilder<T> grant()
    {
        this.voteGranted = true;
        return this;
    }

    public AnyVoteResponseBuilder<T> deny()
    {
        this.voteGranted = false;
        return this;
    }
}
