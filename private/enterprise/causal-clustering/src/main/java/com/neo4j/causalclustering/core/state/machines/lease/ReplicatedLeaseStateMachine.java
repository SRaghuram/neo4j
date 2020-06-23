/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.StateMachine;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.io.state.StateStorage;

import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.util.concurrent.Runnables.EMPTY_RUNNABLE;

/**
 * Listens for {@link ReplicatedLeaseRequest}. Keeps track of the current holder of the lease,
 * which is identified by a monotonically increasing id, and an owning member.
 */
public class ReplicatedLeaseStateMachine implements StateMachine<ReplicatedLeaseRequest>
{
    private final StateStorage<ReplicatedLeaseState> storage;
    private final Runnable runOnLeaseChange;

    private ReplicatedLeaseState state;
    private volatile int leaseId = NO_LEASE;

    public ReplicatedLeaseStateMachine( StateStorage<ReplicatedLeaseState> storage )
    {
        this( storage, EMPTY_RUNNABLE );
    }

    public ReplicatedLeaseStateMachine( StateStorage<ReplicatedLeaseState> storage, Runnable runOnLeaseChange )
    {
        this.storage = storage;
        this.runOnLeaseChange = runOnLeaseChange;
    }

    @Override
    public synchronized void applyCommand( ReplicatedLeaseRequest leaseRequest, long commandIndex,
                                           Consumer<StateMachineResult> callback )
    {
        if ( commandIndex <= state().ordinal() )
        {
            return;
        }

        boolean requestAccepted = leaseRequest.id() == Lease.nextCandidateId( state.leaseId() );
        if ( requestAccepted )
        {
            state = new ReplicatedLeaseState( commandIndex, leaseRequest );
            leaseId = state.leaseId();
            runOnLeaseChange.run();
        }

        callback.accept( StateMachineResult.of( requestAccepted ) );
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

    private ReplicatedLeaseState state()
    {
        if ( state == null )
        {
            state = storage.getInitialState();
            leaseId = state.leaseId();
        }
        return state;
    }

    public synchronized ReplicatedLeaseState snapshot()
    {
        return state().newInstance();
    }

    int leaseId()
    {
        return leaseId;
    }

    public synchronized void installSnapshot( ReplicatedLeaseState snapshot )
    {
        state = snapshot;
    }
}
