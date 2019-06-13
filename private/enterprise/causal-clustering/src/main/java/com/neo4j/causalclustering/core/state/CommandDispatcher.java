/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.util.function.Consumer;

import com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenRequest;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;

public interface CommandDispatcher extends AutoCloseable
{
    void dispatch( ReplicatedTransaction transaction, long commandIndex, Consumer<Result> callback );

    void dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex, Consumer<Result> callback );

    void dispatch( ReplicatedBarrierTokenRequest lockRequest, long commandIndex, Consumer<Result> callback );

    void dispatch( DummyRequest dummyRequest, long commandIndex, Consumer<Result> callback );

    @Override
    void close();
}
