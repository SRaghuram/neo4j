/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.util.Collections;
import java.util.Map;

public class BoundState
{
    private final ClusterId clusterId;
    private final Map<String,CoreSnapshot> coreSnapshotsByDatabaseName;

    BoundState( ClusterId clusterId )
    {
        this( clusterId, Collections.emptyMap() );
    }

    BoundState( ClusterId clusterId, Map<String,CoreSnapshot> coreSnapshotsByDatabaseName )
    {
        this.clusterId = clusterId;
        this.coreSnapshotsByDatabaseName = coreSnapshotsByDatabaseName;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public Map<String,CoreSnapshot> snapshots()
    {
        return coreSnapshotsByDatabaseName;
    }
}
