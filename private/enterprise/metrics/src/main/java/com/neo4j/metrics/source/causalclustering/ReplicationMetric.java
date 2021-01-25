/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;

import java.util.concurrent.atomic.AtomicLong;

public class ReplicationMetric implements ReplicationMonitor
{
    /* total client requests */
    private final AtomicLong newReplication = new AtomicLong();

    /* total active attempts */
    private final AtomicLong attempts = new AtomicLong();

    /* final outcomes */
    private final AtomicLong notReplicated = new AtomicLong();
    private final AtomicLong maybeReplicated = new AtomicLong();
    private final AtomicLong successfullyReplicated = new AtomicLong();

    @Override
    public void clientRequest()
    {
        newReplication.getAndIncrement();
    }

    @Override
    public void replicationAttempt()
    {
        attempts.getAndIncrement();
    }

    @Override
    public void notReplicated()
    {
        notReplicated.getAndIncrement();
    }

    @Override
    public void maybeReplicated()
    {
        maybeReplicated.getAndIncrement();
    }

    @Override
    public void successfullyReplicated()
    {
        successfullyReplicated.getAndIncrement();
    }

    long newReplicationCount()
    {
        return newReplication.get();
    }

    long attemptCount()
    {
        return attempts.get();
    }

    long failCount()
    {
        return notReplicated.get();
    }

    long maybeCount()
    {
        return maybeReplicated.get();
    }

    long successCount()
    {
        return successfullyReplicated.get();
    }
}
