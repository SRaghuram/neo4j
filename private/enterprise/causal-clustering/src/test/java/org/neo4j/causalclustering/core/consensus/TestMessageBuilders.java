/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import org.neo4j.causalclustering.core.consensus.roles.AppendEntriesRequestBuilder;
import org.neo4j.causalclustering.core.consensus.roles.AppendEntriesResponseBuilder;
import org.neo4j.causalclustering.core.consensus.vote.PreVoteRequestBuilder;
import org.neo4j.causalclustering.core.consensus.vote.PreVoteResponseBuilder;
import org.neo4j.causalclustering.core.consensus.vote.VoteRequestBuilder;
import org.neo4j.causalclustering.core.consensus.vote.VoteResponseBuilder;

public class TestMessageBuilders
{
    private TestMessageBuilders()
    {
    }

    public static AppendEntriesRequestBuilder appendEntriesRequest()
    {
        return new AppendEntriesRequestBuilder();
    }

    public static AppendEntriesResponseBuilder appendEntriesResponse()
    {
        return new AppendEntriesResponseBuilder();
    }

    public static HeartbeatBuilder heartbeat()
    {
        return new HeartbeatBuilder();
    }

    public static VoteRequestBuilder voteRequest()
    {
        return new VoteRequestBuilder();
    }

    public static PreVoteRequestBuilder preVoteRequest()
    {
        return new PreVoteRequestBuilder();
    }

    public static VoteResponseBuilder voteResponse()
    {
        return new VoteResponseBuilder();
    }

    public static PreVoteResponseBuilder preVoteResponse()
    {
        return new PreVoteResponseBuilder();
    }
}
