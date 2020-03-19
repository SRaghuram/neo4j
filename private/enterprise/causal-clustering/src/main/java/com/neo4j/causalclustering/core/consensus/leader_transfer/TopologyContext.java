/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.Set;

import org.neo4j.kernel.database.DatabaseId;

class TopologyContext
{
    private final DatabaseId databaseId;
    private RaftId raftId;
    private final Set<MemberId> members;

    TopologyContext( DatabaseId databaseId, RaftId raftId, Set<MemberId> members )
    {
        this.databaseId = databaseId;
        this.raftId = raftId;
        this.members = members;
    }

    public Set<MemberId> members()
    {
        return members;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public RaftId raftId()
    {
        return raftId;
    }
}
