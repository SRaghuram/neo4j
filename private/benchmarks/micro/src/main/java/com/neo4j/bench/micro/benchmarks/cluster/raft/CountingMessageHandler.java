/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.messaging.Inbound;

class CountingMessageHandler implements Inbound.MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
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
    public void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message )
    {
        int left = expectedCount.decrementAndGet();
        if ( left == 0 )
        {
            currentFuture.complete( null );
        }
    }
}
