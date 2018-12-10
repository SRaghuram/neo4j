/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.state.Result;

import java.util.concurrent.Semaphore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The progress of a single replicated operation, from replication to result, and associated synchronization.
 */
public class Progress
{
    private final Semaphore replicationSignal = new Semaphore( 0 );
    private final Semaphore resultSignal = new Semaphore( 0 );

    private volatile boolean isReplicated;
    private volatile Result result;

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

    public void awaitResult() throws InterruptedException
    {
        if ( this.result == null )
        {
            resultSignal.acquire();
        }
    }

    public boolean isReplicated()
    {
        return isReplicated;
    }

    void registerResult( Result result )
    {
        this.result = result;
        resultSignal.release();
    }

    public Result result()
    {
        return result;
    }
}
