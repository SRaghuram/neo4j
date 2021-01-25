/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogAppendIndexMonitor;

import java.io.IOException;

import org.neo4j.monitoring.Monitors;

public class MonitoredRaftLog extends DelegatingRaftLog
{
    private final RaftLogAppendIndexMonitor appendIndexMonitor;

    public MonitoredRaftLog( RaftLog delegate, Monitors monitors )
    {
        super( delegate );
        this.appendIndexMonitor = monitors.newMonitor( RaftLogAppendIndexMonitor.class, getClass().getName() );
    }

    @Override
    public long append( RaftLogEntry... entries ) throws IOException
    {
        long appendIndex = super.append( entries );
        appendIndexMonitor.appendIndex( appendIndex );
        return appendIndex;
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        super.truncate( fromIndex );
        appendIndexMonitor.appendIndex( super.appendIndex() );
    }
}
