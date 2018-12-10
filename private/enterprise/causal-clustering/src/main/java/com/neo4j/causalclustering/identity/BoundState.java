/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.util.Optional;

public class BoundState
{
    private final ClusterId clusterId;
    private final CoreSnapshot snapshot;

    BoundState( ClusterId clusterId )
    {
        this( clusterId, null );
    }

    BoundState( ClusterId clusterId, CoreSnapshot snapshot )
    {
        this.clusterId = clusterId;
        this.snapshot = snapshot;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public Optional<CoreSnapshot> snapshot()
    {
        return Optional.ofNullable( snapshot );
    }
}
