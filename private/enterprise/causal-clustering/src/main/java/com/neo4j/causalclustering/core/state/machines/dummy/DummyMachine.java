/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.dummy;

import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.core.state.machines.StateMachine;

import java.util.function.Consumer;

public class DummyMachine implements StateMachine<DummyRequest>
{
    @Override
    public void applyCommand( DummyRequest dummyRequest, long commandIndex, Consumer<Result> callback )
    {
        callback.accept( Result.of( dummyRequest ) );
    }

    @Override
    public void flush()
    {
    }

    @Override
    public long lastAppliedIndex()
    {
        return 0;
    }
}
