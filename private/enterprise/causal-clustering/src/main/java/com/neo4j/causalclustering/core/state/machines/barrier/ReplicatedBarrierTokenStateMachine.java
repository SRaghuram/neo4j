/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.core.state.machines.StateMachine;
import com.neo4j.causalclustering.core.state.storage.StateStorage;

import java.io.IOException;
import java.util.function.Consumer;

import static org.neo4j.util.concurrent.Runnables.EMPTY_RUNNABLE;

/**
 * Listens for {@link ReplicatedBarrierTokenRequest}. Keeps track of the current holder of the replicated token,
 * which is identified by a monotonically increasing id, and an owning member.
 */
public class ReplicatedBarrierTokenStateMachine implements StateMachine<ReplicatedBarrierTokenRequest>
{
    private final StateStorage<ReplicatedBarrierTokenState> storage;
    private final Runnable runOnTokenChange;

    private ReplicatedBarrierTokenState state;

    public ReplicatedBarrierTokenStateMachine( StateStorage<ReplicatedBarrierTokenState> storage )
    {
        this( storage, EMPTY_RUNNABLE );
    }

    public ReplicatedBarrierTokenStateMachine( StateStorage<ReplicatedBarrierTokenState> storage, Runnable runOnTokenChange )
    {
        this.storage = storage;
        this.runOnTokenChange = runOnTokenChange;
    }

    @Override
    public synchronized void applyCommand( ReplicatedBarrierTokenRequest tokenRequest, long commandIndex,
                                           Consumer<Result> callback )
    {
        if ( commandIndex <= state().ordinal() )
        {
            return;
        }

        boolean requestAccepted = tokenRequest.id() == BarrierToken.nextCandidateId( state.candidateId() );
        if ( requestAccepted )
        {
            state = new ReplicatedBarrierTokenState( commandIndex, tokenRequest );
            runOnTokenChange.run();
        }

        callback.accept( Result.of( requestAccepted ) );
    }

    @Override
    public synchronized void flush() throws IOException
    {
        storage.writeState( state() );
    }

    @Override
    public long lastAppliedIndex()
    {
        return state().ordinal();
    }

    private ReplicatedBarrierTokenState state()
    {
        if ( state == null )
        {
            state = storage.getInitialState();
        }
        return state;
    }

    public synchronized ReplicatedBarrierTokenState snapshot()
    {
        return state().newInstance();
    }

    public synchronized int candidateId()
    {
        return state().candidateId();
    }

    public synchronized void installSnapshot( ReplicatedBarrierTokenState snapshot )
    {
        state = snapshot;
    }
}
