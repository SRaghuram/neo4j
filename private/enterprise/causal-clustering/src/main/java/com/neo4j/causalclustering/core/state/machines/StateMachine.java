/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines;

import com.neo4j.causalclustering.core.state.StateMachineResult;

import java.io.IOException;
import java.util.function.Consumer;

public interface StateMachine<Command>
{
    /**
     * Apply command to state machine, modifying its internal state.
     * Implementations should be idempotent, so that the caller is free to replay commands from any point in the log.
     * @param command Command to the state machine.
     * @param commandIndex The index of the command.
     * @param callback To be called when a result is produced.
     */
    void applyCommand( Command command, long commandIndex, Consumer<StateMachineResult> callback );

    /**
     * Flushes state to durable storage.
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Return the index of the last applied command by this state machine.
     * @return the last applied index for this state machine
     */
    long lastAppliedIndex();
}
