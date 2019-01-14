/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.causalclustering;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.causalclustering.core.consensus.log.monitoring.RaftTermMonitor;

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
