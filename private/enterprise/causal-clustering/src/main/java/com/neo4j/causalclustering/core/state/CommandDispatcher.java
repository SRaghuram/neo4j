/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.util.function.Consumer;

import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.status.StatusRequest;

public interface CommandDispatcher extends AutoCloseable
{
    void dispatch( ReplicatedTransaction transaction, long commandIndex, Consumer<StateMachineResult> callback );

    void dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex, Consumer<StateMachineResult> callback );

    void dispatch( ReplicatedLeaseRequest leaseRequest, long commandIndex, Consumer<StateMachineResult> callback );

    void dispatch( DummyRequest dummyRequest, long commandIndex, Consumer<StateMachineResult> callback );

    void dispatch( StatusRequest statusRequest, long commandIndex, Consumer<StateMachineResult> callback );

    @Override
    void close();
}
