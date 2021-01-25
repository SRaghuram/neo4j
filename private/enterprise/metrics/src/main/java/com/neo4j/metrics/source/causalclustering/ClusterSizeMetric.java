/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Gauge;
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;

public class ClusterSizeMetric implements ClusterSizeMonitor
{
    private volatile int members;
    private volatile int unreachable;
    private volatile boolean converged;

    @Override
    public void setMembers( int size )
    {
        this.members = size;
    }

    @Override
    public void setUnreachable( int size )
    {
        this.unreachable = size;
    }

    @Override
    public void setConverged( boolean converged )
    {
        this.converged = converged;
    }

    public Gauge<Integer> members()
    {
        return () -> members;
    }

    public Gauge<Integer> unreachable()
    {
        return () -> unreachable;
    }

    public Gauge<Integer> converged()
    {
        return () -> converged ? 1 : 0;
    }
}
