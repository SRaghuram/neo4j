/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.vote;

import org.neo4j.causalclustering.core.consensus.RaftMessages;

public class VoteResponseBuilder extends AnyVoteResponseBuilder<RaftMessages.Vote.Response>
{
    public VoteResponseBuilder()
    {
        super( RaftMessages.Vote.Response::new );
    }
}
