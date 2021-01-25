/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogAppendIndexMonitor;

import java.util.concurrent.atomic.AtomicLong;

public class RaftLogAppendIndexMetric implements RaftLogAppendIndexMonitor
{
    private AtomicLong appendIndex = new AtomicLong( 0 );

    @Override
    public long appendIndex()
    {
        return appendIndex.get();
    }

    @Override
    public void appendIndex( long appendIndex )
    {
        this.appendIndex.set( appendIndex );
    }
}
