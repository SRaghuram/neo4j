/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import com.neo4j.causalclustering.helper.SuspendableLifecycleStateTestHelpers.LifeCycleState;

import org.neo4j.logging.Log;

public class StateAwareSuspendableLifeCycle extends SuspendableLifeCycle
{
    public LifeCycleState status;

    StateAwareSuspendableLifeCycle( Log debugLog )
    {
        super( debugLog );
    }

    @Override
    protected void start0()
    {
        status = LifeCycleState.Start;
    }

    @Override
    protected void stop0()
    {
        status = LifeCycleState.Stop;
    }

    @Override
    protected void shutdown0()
    {
        status = LifeCycleState.Shutdown;
    }

    @Override
    protected void init0()
    {
        status = LifeCycleState.Init;
    }
}
