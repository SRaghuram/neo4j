/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.Set;

import org.neo4j.kernel.database.NamedDatabaseId;

class TransferCandidates
{
    private final NamedDatabaseId namedDatabaseId;
    private RaftId raftId;
    private final Set<MemberId> members;

    TransferCandidates( NamedDatabaseId namedDatabaseId, RaftId raftId, Set<MemberId> members )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.raftId = raftId;
        this.members = members;
    }

    public Set<MemberId> members()
    {
        return members;
    }

    public NamedDatabaseId databaseId()
    {
        return namedDatabaseId;
    }

    public RaftId raftId()
    {
        return raftId;
    }
}
