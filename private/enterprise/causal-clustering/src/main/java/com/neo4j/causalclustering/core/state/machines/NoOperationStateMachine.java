/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines;

import com.neo4j.causalclustering.core.state.StateMachineResult;

import java.util.function.Consumer;

public class NoOperationStateMachine<T extends NoOperationRequest> implements StateMachine<T>
{
    @Override
    public void applyCommand( T noOperationRequest, long commandIndex, Consumer<StateMachineResult> callback )
    {
        callback.accept( StateMachineResult.of( noOperationRequest ) );
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
