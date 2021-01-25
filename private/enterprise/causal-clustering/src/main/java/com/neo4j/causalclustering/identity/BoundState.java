/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.util.Optional;

public class BoundState
{
    private final RaftGroupId raftGroupId;
    private final CoreSnapshot coreSnapshot;

    public BoundState( RaftGroupId raftGroupId )
    {
        this( raftGroupId, null );
    }

    BoundState( RaftGroupId raftGroupId, CoreSnapshot coreSnapshot )
    {
        this.raftGroupId = raftGroupId;
        this.coreSnapshot = coreSnapshot;
    }

    public RaftGroupId raftGroupId()
    {
        return raftGroupId;
    }

    public Optional<CoreSnapshot> snapshot()
    {
        return Optional.ofNullable( coreSnapshot );
    }
}
