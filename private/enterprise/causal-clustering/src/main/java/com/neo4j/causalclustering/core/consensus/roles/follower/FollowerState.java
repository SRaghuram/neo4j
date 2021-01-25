/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles.follower;

import static java.lang.String.format;

/**
 * Things the leader thinks it knows about a follower.
 */
public class FollowerState
{
    // We know that the follower agrees with our (leader) log up until this index. Only updated by the leader when:
    // * increased when it receives a successful AppendEntries.Response
    private final long matchIndex;

    public FollowerState()
    {
        this( -1 );
    }

    private FollowerState( long matchIndex )
    {
        assert matchIndex >= -1 : format( "Match index can never be less than -1. Was %d", matchIndex );
        this.matchIndex = matchIndex;
    }

    public long getMatchIndex()
    {
        return matchIndex;
    }

    public FollowerState onSuccessResponse( long newMatchIndex )
    {
        return new FollowerState( newMatchIndex );
    }

    @Override
    public String toString()
    {
        return format( "State{matchIndex=%d}", matchIndex );
    }
}
