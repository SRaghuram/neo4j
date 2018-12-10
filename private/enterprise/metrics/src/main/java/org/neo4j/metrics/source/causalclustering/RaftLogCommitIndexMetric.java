/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.causalclustering;

import java.util.concurrent.atomic.AtomicLong;

import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;

public class RaftLogCommitIndexMetric implements RaftLogCommitIndexMonitor
{
    private AtomicLong commitIndex = new AtomicLong( 0 );

    @Override
    public long commitIndex()
    {
        return commitIndex.get();
    }

    @Override
    public void commitIndex( long commitIndex )
    {
        this.commitIndex.set( commitIndex );
    }
}
