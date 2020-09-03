/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

class LeaderTransferTarget
{
    static final LeaderTransferTarget NO_TARGET = new LeaderTransferTarget( null, null );
    private final NamedDatabaseId namedDatabaseId;
    private final ServerId serverId;

    LeaderTransferTarget( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.serverId = serverId;
    }

    ServerId to()
    {
        return serverId;
    }

    NamedDatabaseId databaseId()
    {
        return namedDatabaseId;
    }
}
