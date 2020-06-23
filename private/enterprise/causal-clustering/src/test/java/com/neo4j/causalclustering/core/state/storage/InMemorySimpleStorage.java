/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import org.neo4j.io.state.SimpleStorage;

public class InMemorySimpleStorage<T> implements SimpleStorage<T>
{
    private T state;

    @Override
    public boolean exists()
    {
        return state != null;
    }

    @Override
    public T readState()
    {
        return state;
    }

    @Override
    public void writeState( T state )
    {
        this.state = state;
    }
}
