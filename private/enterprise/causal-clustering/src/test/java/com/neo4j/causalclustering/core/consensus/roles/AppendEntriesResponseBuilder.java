/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftMemberId;

public class AppendEntriesResponseBuilder
{
    private boolean success;
    private long term = -1;
    private RaftMemberId from;
    private long matchIndex = -1;
    private long appendIndex = -1;

    public RaftMessages.AppendEntries.Response build()
    {
        // a response of false should always have a match index of -1
        assert success || matchIndex == -1;
        return new RaftMessages.AppendEntries.Response( from, term, success, matchIndex, appendIndex );
    }

    public AppendEntriesResponseBuilder from( RaftMemberId from )
    {
        this.from = from;
        return this;
    }

    public AppendEntriesResponseBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public AppendEntriesResponseBuilder matchIndex( long matchIndex )
    {
        this.matchIndex = matchIndex;
        return this;
    }

    public AppendEntriesResponseBuilder appendIndex( long appendIndex )
    {
        this.appendIndex = appendIndex;
        return this;
    }

    public AppendEntriesResponseBuilder success()
    {
        this.success = true;
        return this;
    }

    public AppendEntriesResponseBuilder failure()
    {
        this.success = false;
        return this;
    }
}
