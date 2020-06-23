/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.SessionTracker;
import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.io.IOException;

import org.neo4j.io.state.StateStorage;

import static java.lang.Long.max;

public class CoreState
{
    private final SessionTracker sessionTracker;
    private final StateStorage<Long> lastFlushedStorage;
    private final CoreStateMachines stateMachines;

    CoreState( SessionTracker sessionTracker, StateStorage<Long> lastFlushedStorage, CoreStateMachines stateMachines )
    {
        this.sessionTracker = sessionTracker;
        this.lastFlushedStorage = lastFlushedStorage;
        this.stateMachines = stateMachines;
    }

    public void augmentSnapshot( CoreSnapshot coreSnapshot )
    {
        stateMachines.augmentSnapshot( coreSnapshot );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, sessionTracker.snapshot() );
    }

    public void installSnapshot( CoreSnapshot coreSnapshot )
    {
        stateMachines.installSnapshot( coreSnapshot );
        sessionTracker.installSnapshot( coreSnapshot.get( CoreStateFiles.SESSION_TRACKER ) );
    }

    public void flush( long lastApplied ) throws IOException
    {
        stateMachines.flush();
        sessionTracker.flush();
        lastFlushedStorage.writeState( lastApplied );
    }

    public CommandDispatcher commandDispatcher()
    {
        return stateMachines.commandDispatcher();
    }

    public long getLastAppliedIndex()
    {
        long lastAppliedIndex = stateMachines.getLastAppliedIndex();
        long maxFromSession = sessionTracker.getLastAppliedIndex();
        return max( lastAppliedIndex, maxFromSession );
    }

    public long getLastFlushed()
    {
        return lastFlushedStorage.getInitialState();
    }
}
