/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.StateMachine;

import java.util.concurrent.atomic.AtomicReference;

public class DirectReplicator<Command extends ReplicatedContent> implements Replicator
{
    private final StateMachine<Command> stateMachine;
    private long commandIndex;

    public DirectReplicator( StateMachine<Command> stateMachine )
    {
        this.stateMachine = stateMachine;
    }

    @Override
    public synchronized ReplicationResult replicate( ReplicatedContent content )
    {
        AtomicReference<StateMachineResult> atomicResult = new AtomicReference<>();
        //noinspection unchecked
        stateMachine.applyCommand( (Command) content, commandIndex++, atomicResult::set );

        try
        {
            return ReplicationResult.applied( atomicResult.get() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
