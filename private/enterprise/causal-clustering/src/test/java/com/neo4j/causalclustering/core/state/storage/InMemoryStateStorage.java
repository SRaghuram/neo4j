/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import org.neo4j.io.state.StateStorage;

public class InMemoryStateStorage<STATE> implements StateStorage<STATE>
{
    private STATE state;

    public InMemoryStateStorage( STATE state )
    {
        this.state = state;
    }

    @Override
    public STATE getInitialState()
    {
        return state;
    }

    @Override
    public void writeState( STATE state )
    {
        this.state = state;
    }

    @Override
    public boolean exists()
    {
        return true;
    }
}
