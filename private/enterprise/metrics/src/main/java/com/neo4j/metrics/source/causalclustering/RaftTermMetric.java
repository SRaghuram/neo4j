/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftTermMonitor;

import java.util.concurrent.atomic.AtomicLong;

public class RaftTermMetric implements RaftTermMonitor
{
    private AtomicLong term = new AtomicLong( 0 );

    @Override
    public long term()
    {
        return term.get();
    }

    @Override
    public void term( long term )
    {
        this.term.set( term );
    }
}
