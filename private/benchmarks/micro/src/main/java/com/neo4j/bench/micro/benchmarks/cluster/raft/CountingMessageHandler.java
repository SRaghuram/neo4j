/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.messaging.Inbound;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

class CountingMessageHandler implements Inbound.MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>
{
    private final AtomicInteger expectedCount;
    private final CompletableFuture<Void> currentFuture = new CompletableFuture<>();

    CountingMessageHandler( int expectedCount )
    {
        this.expectedCount = new AtomicInteger( expectedCount );
    }

    CompletableFuture<Void> future()
    {
        return currentFuture;
    }

    @Override
    public void handle( RaftMessages.ReceivedInstantRaftIdAwareMessage<?> message )
    {
        int left = expectedCount.decrementAndGet();
        if ( left == 0 )
        {
            currentFuture.complete( null );
        }
    }
}
