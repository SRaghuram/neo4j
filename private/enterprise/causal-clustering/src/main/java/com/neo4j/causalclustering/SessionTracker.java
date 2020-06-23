/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering;

import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.replication.session.LocalOperationId;

import java.io.IOException;

import org.neo4j.io.state.StateStorage;

public class SessionTracker
{
    private final StateStorage<GlobalSessionTrackerState> sessionTrackerStorage;
    private GlobalSessionTrackerState sessionState;

    public SessionTracker( StateStorage<GlobalSessionTrackerState> sessionTrackerStorage )
    {
        this.sessionTrackerStorage = sessionTrackerStorage;
    }

    public void start()
    {
        if ( sessionState == null )
        {
            sessionState = sessionTrackerStorage.getInitialState();
        }
    }

    public long getLastAppliedIndex()
    {
        return sessionState.logIndex();
    }

    public void flush() throws IOException
    {
        sessionTrackerStorage.writeState( sessionState );
    }

    public GlobalSessionTrackerState snapshot()
    {
        return sessionState.newInstance();
    }

    public void installSnapshot( GlobalSessionTrackerState sessionState )
    {
        this.sessionState = sessionState;
    }

    public boolean validateOperation( GlobalSession globalSession, LocalOperationId localOperationId )
    {
        return sessionState.validateOperation( globalSession, localOperationId );
    }

    public void update( GlobalSession globalSession, LocalOperationId localOperationId, long logIndex )
    {
        sessionState.update( globalSession, localOperationId, logIndex );
    }

    public GlobalSessionTrackerState newInstance()
    {
        return sessionState.newInstance();
    }
}
