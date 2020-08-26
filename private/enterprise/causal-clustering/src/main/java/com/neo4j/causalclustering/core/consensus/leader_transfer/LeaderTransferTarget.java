/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.RaftMemberId;

import org.neo4j.kernel.database.NamedDatabaseId;

class LeaderTransferTarget
{
    static final LeaderTransferTarget NO_TARGET = new LeaderTransferTarget( null, null );
    private final NamedDatabaseId namedDatabaseId;
    private final RaftMemberId raftMemberId;

    LeaderTransferTarget( NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.raftMemberId = raftMemberId;
    }

    RaftMemberId to()
    {
        return raftMemberId;
    }

    NamedDatabaseId databaseId()
    {
        return namedDatabaseId;
    }
}
