/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.core.consensus.RaftMessages;

public class PreVoteRequestBuilder extends AnyVoteRequestBuilder<RaftMessages.PreVote.Request>
{
    public PreVoteRequestBuilder()
    {
        super( RaftMessages.PreVote.Request::new );
    }
}
