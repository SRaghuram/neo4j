/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftGroup;
import com.neo4j.causalclustering.core.replication.ProgressTracker;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.identity.RaftBinder;

class CoreRaftContext
{
    private final RaftGroup raftGroup;
    private final Replicator replicator;
    private final CommandIndexTracker commandIndexTracker;
    private final ProgressTracker progressTracker;
    private final RaftBinder raftBinder;

    CoreRaftContext( RaftGroup raftGroup, Replicator replicator, CommandIndexTracker commandIndexTracker, ProgressTracker progressTracker,
            RaftBinder raftBinder )
    {
        this.raftGroup = raftGroup;
        this.replicator = replicator;
        this.commandIndexTracker = commandIndexTracker;
        this.progressTracker = progressTracker;
        this.raftBinder = raftBinder;
    }

    RaftGroup raftGroup()
    {
        return raftGroup;
    }

    Replicator replicator()
    {
        return replicator;
    }

    CommandIndexTracker commandIndexTracker()
    {
        return commandIndexTracker;
    }

    ProgressTracker progressTracker()
    {
        return progressTracker;
    }

    RaftBinder raftBinder()
    {
        return raftBinder;
    }
}
