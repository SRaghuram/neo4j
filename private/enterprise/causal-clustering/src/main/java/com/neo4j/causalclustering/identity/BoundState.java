/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.util.Optional;

public class BoundState
{
    private final RaftId raftId;
    private final CoreSnapshot coreSnapshot;

    public BoundState( RaftId raftId )
    {
        this( raftId, null );
    }

    BoundState( RaftId raftId, CoreSnapshot coreSnapshot )
    {
        this.raftId = raftId;
        this.coreSnapshot = coreSnapshot;
    }

    public RaftId raftId()
    {
        return raftId;
    }

    public Optional<CoreSnapshot> snapshot()
    {
        return Optional.ofNullable( coreSnapshot );
    }
}
