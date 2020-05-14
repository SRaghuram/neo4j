/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Gauge;
import com.neo4j.causalclustering.core.consensus.CoreMetaData;
import com.neo4j.causalclustering.core.consensus.RaftMessageTimerResetMonitor;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.time.Stopwatch;

class LastLeaderMessageMetric implements RaftMessageTimerResetMonitor, Gauge<Long>
{
    private final Supplier<CoreMetaData> coreMetaData;
    private volatile Stopwatch lastTime;

    LastLeaderMessageMetric( Supplier<CoreMetaData> coreMetaData )
    {
        this.coreMetaData = coreMetaData;
    }

    @Override
    public void timerReset()
    {
        lastTime = Stopwatch.start();
    }

    @Override
    public Long getValue()
    {
        return coreMetaData.get().isLeader() ? 0L : lastTime.elapsed( TimeUnit.MILLISECONDS );
    }
}
