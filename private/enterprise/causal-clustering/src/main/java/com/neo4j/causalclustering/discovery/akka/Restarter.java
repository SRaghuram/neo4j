/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import java.util.function.Supplier;

import org.neo4j.internal.helpers.TimeoutStrategy;

public class Restarter
{
    private final TimeoutStrategy timeoutStrategy;
    private final int acceptableFailures;
    private volatile boolean healthy = true;

    public Restarter( TimeoutStrategy timeoutStrategy, int acceptableFailures )
    {
        this.timeoutStrategy = timeoutStrategy;

        this.acceptableFailures = acceptableFailures;
    }

    public boolean isHealthy()
    {
        return healthy;
    }

    public void restart( Supplier<Boolean> retry )
    {
        int count = 0;
        TimeoutStrategy.Timeout timeout = timeoutStrategy.newTimeout();
        while ( !retry.get() )
        {
            if ( count > acceptableFailures )
            {
                healthy = false;
            }
            count++;
            try
            {
                Thread.sleep( timeout.getAndIncrement() );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
        }
        healthy = true;
    }
}
