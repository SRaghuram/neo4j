/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The progress of a single replicated operation, from replication to result, and associated synchronization.
 */
public class Progress
{
    private final Semaphore replicationSignal = new Semaphore( 0 );
    private final CompletableFuture<Object> futureResult = new CompletableFuture<>();

    private volatile boolean isReplicated;

    public void triggerReplicationEvent()
    {
        replicationSignal.release();
    }

    public void setReplicated()
    {
        isReplicated = true;
        replicationSignal.release();
    }

    public void awaitReplication( long timeoutMillis ) throws InterruptedException
    {
        if ( !isReplicated )
        {
            replicationSignal.tryAcquire( timeoutMillis, MILLISECONDS );
        }
    }

    public boolean isReplicated()
    {
        return isReplicated;
    }

    public CompletableFuture<Object> futureResult()
    {
        return futureResult;
    }
}
