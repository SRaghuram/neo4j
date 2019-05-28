/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import java.io.IOException;
import java.util.function.Consumer;

import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.core.state.machines.StateMachine;
import com.neo4j.causalclustering.core.state.storage.StateStorage;

/**
 * Listens for {@link ReplicatedBarrierTokenRequest}. Keeps track of the current holder of the replicated token,
 * which is identified by a monotonically increasing id, and an owning member.
 */
public class ReplicatedBarrierTokenStateMachine implements StateMachine<ReplicatedBarrierTokenRequest>
{
    private final StateStorage<ReplicatedBarrierTokenState> storage;

    private ReplicatedBarrierTokenState state;

    public ReplicatedBarrierTokenStateMachine( StateStorage<ReplicatedBarrierTokenState> storage )
    {
        this.storage = storage;
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

    public synchronized void installSnapshot( ReplicatedBarrierTokenState snapshot )
    {
        state = snapshot;
    }

}
