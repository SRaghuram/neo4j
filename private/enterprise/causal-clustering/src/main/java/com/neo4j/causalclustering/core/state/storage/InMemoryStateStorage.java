/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

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
    public void persistStoreData( STATE state )
    {
        this.state = state;
    }
}
