/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.server;

import com.neo4j.bench.agent.AgentState.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public abstract class AgentCommand<R> implements Supplier<R>
{
    private static final Logger LOG = LoggerFactory.getLogger( AgentCommand.class );

    AgentLifecycle agentLifecycle;

    AgentCommand( AgentLifecycle agentLifecycle )
    {
        this.agentLifecycle = agentLifecycle;
    }

    abstract State requiredState();

    abstract State resultState();

    abstract R perform();

    @Override
    public R get()
    {
        agentLifecycle.assertState( requiredState() );
        try
        {
            return perform();
        }
        finally
        {
            after();
            agentLifecycle.setState( resultState() );
        }
    }

    void after()
    {
        // no-op
    }
}
