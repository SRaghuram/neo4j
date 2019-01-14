/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.causalclustering;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;

public class ReplicationMetric implements ReplicationMonitor
{
    private final AtomicLong  newReplication = new AtomicLong(  );
    private final AtomicLong attempts = new AtomicLong(  );
    private final AtomicLong success = new AtomicLong(  );
    private final AtomicLong fail = new AtomicLong(  );

    @Override
    public void startReplication()
    {
        newReplication.getAndIncrement();
    }

    @Override
    public void replicationAttempt()
    {
        attempts.getAndIncrement();
    }

    @Override
    public void successfulReplication()
    {
        success.getAndIncrement();
    }

    @Override
    public void failedReplication( Throwable t )
    {
        fail.getAndIncrement();
    }

    long newReplicationCount()
    {
        return newReplication.get();
    }

    long attemptCount()
    {
        return attempts.get();
    }

    long successCount()
    {
        return success.get();
    }

    long failCount()
    {
        return fail.get();
    }
}
