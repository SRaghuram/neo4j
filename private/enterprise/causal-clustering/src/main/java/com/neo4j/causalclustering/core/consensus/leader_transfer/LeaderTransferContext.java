/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import org.neo4j.kernel.database.NamedDatabaseId;

class LeaderTransferContext
{
    static final LeaderTransferContext NO_TARGET = new LeaderTransferContext( null, null, null );
    private NamedDatabaseId namedDatabaseId;
    private final RaftId raftId;
    private final MemberId memberId;

    LeaderTransferContext( NamedDatabaseId namedDatabaseId, RaftId raftId, MemberId memberId )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.raftId = raftId;
        this.memberId = memberId;
    }

    MemberId to()
    {
        return memberId;
    }

    RaftId raftId()
    {
        return raftId;
    }

    NamedDatabaseId databaseId()
    {
        return namedDatabaseId;
    }
}
