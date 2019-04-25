/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.util.Collections;
import java.util.Map;

import org.neo4j.kernel.database.DatabaseId;

public class BoundState
{
    private final ClusterId clusterId;
    private final Map<DatabaseId,CoreSnapshot> coreSnapshotsByDatabaseId;

    BoundState( ClusterId clusterId )
    {
        this( clusterId, Collections.emptyMap() );
    }

    BoundState( ClusterId clusterId, Map<DatabaseId,CoreSnapshot> coreSnapshotsByDatabaseId )
    {
        this.clusterId = clusterId;
        this.coreSnapshotsByDatabaseId = coreSnapshotsByDatabaseId;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public Map<DatabaseId,CoreSnapshot> snapshots()
    {
        return coreSnapshotsByDatabaseId;
    }
}
