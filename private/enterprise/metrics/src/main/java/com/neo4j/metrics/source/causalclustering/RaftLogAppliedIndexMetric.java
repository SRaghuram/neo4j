/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogAppliedIndexMonitor;

import java.util.concurrent.atomic.AtomicLong;

public class RaftLogAppliedIndexMetric implements RaftLogAppliedIndexMonitor
{
    private AtomicLong appliedIndex = new AtomicLong( 0 );

    @Override
    public long appliedIndex()
    {
        return appliedIndex.get();
    }

    @Override
    public void appliedIndex( long appliedIndex )
    {
        this.appliedIndex.set( appliedIndex );
    }
}
