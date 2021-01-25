/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessageHandler;

public enum Role
{
    FOLLOWER( new Follower() ),
    CANDIDATE( new Candidate() ),
    LEADER( new Leader() );

    public final RaftMessageHandler handler;

    Role( RaftMessageHandler handler )
    {
        this.handler = handler;
    }
}
