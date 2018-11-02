/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.roles;

import org.neo4j.causalclustering.core.consensus.RaftMessageHandler;

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
