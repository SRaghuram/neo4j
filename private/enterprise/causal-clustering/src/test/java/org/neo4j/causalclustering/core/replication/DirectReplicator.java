/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.StateMachine;

public class DirectReplicator<Command extends ReplicatedContent> implements Replicator
{
    private final StateMachine<Command> stateMachine;
    private long commandIndex;

    public DirectReplicator( StateMachine<Command> stateMachine )
    {
        this.stateMachine = stateMachine;
    }

    @Override
    public synchronized Result replicate( ReplicatedContent content )
    {
        AtomicReference<Result> atomicResult = new AtomicReference<>();
        stateMachine.applyCommand( (Command) content, commandIndex++, atomicResult::set );

        try
        {
            return atomicResult.get();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
