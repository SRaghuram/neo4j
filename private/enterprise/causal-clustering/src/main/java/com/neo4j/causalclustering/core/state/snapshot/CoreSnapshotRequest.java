/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import org.neo4j.kernel.database.DatabaseId;

public class CoreSnapshotRequest extends CatchupProtocolMessage.WithDatabaseId
{
    public CoreSnapshotRequest( DatabaseId databaseId )
    {
        super( RequestMessageType.CORE_SNAPSHOT, databaseId );
    }
}
