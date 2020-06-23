/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.term;

import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftTermMonitor;

import java.io.IOException;

import org.neo4j.io.state.StateStorage;
import org.neo4j.monitoring.Monitors;

public class MonitoredTermStateStorage implements StateStorage<TermState>
{
    private static final String TERM_TAG = "term";

    private final StateStorage<TermState> delegate;
    private final RaftTermMonitor termMonitor;

    public MonitoredTermStateStorage( StateStorage<TermState> delegate, Monitors monitors )
    {
        this.delegate = delegate;
        this.termMonitor = monitors.newMonitor( RaftTermMonitor.class, getClass().getName(), TERM_TAG );
    }

    @Override
    public TermState getInitialState()
    {
        return delegate.getInitialState();
    }

    @Override
    public void writeState( TermState state ) throws IOException
    {
        delegate.writeState( state );
        termMonitor.term( state.currentTerm() );
    }

    @Override
    public boolean exists()
    {
        return delegate.exists();
    }
}
