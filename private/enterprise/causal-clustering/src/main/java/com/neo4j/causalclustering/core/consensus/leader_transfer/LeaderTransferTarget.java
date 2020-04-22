/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;

import org.neo4j.kernel.database.NamedDatabaseId;

class LeaderTransferTarget
{
    static final LeaderTransferTarget NO_TARGET = new LeaderTransferTarget( null, null );
    private final NamedDatabaseId namedDatabaseId;
    private final MemberId memberId;

    LeaderTransferTarget( NamedDatabaseId namedDatabaseId, MemberId memberId )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.memberId = memberId;
    }

    MemberId to()
    {
        return memberId;
    }

    NamedDatabaseId databaseId()
    {
        return namedDatabaseId;
    }
}
