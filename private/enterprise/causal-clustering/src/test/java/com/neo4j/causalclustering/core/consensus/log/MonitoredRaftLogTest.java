/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogAppendIndexMonitor;
import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;
import org.junit.Test;

import org.neo4j.monitoring.Monitors;

import static org.junit.Assert.assertEquals;

public class MonitoredRaftLogTest
{
    @Test
    public void shouldMonitorAppendIndexAndCommitIndex() throws Exception
    {
        // Given
        Monitors monitors = new Monitors();
        StubRaftLogAppendIndexMonitor appendMonitor = new StubRaftLogAppendIndexMonitor();
        monitors.addMonitorListener( appendMonitor );

        StubRaftLogCommitIndexMonitor commitMonitor = new StubRaftLogCommitIndexMonitor();
        monitors.addMonitorListener( commitMonitor );

        MonitoredRaftLog log = new MonitoredRaftLog( new InMemoryRaftLog(), monitors );

        // When
        log.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1 ) ) );
        log.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1 ) ) );

        assertEquals( 1, appendMonitor.appendIndex() );
        assertEquals( 0, commitMonitor.commitIndex() );

        log.truncate( 1 );
        assertEquals( 0, appendMonitor.appendIndex() );
    }

    private static class StubRaftLogCommitIndexMonitor implements RaftLogCommitIndexMonitor
    {
        private long commitIndex;

        @Override
        public long commitIndex()
        {
            return commitIndex;
        }

        @Override
        public void commitIndex( long commitIndex )
        {
            this.commitIndex = commitIndex;
        }
    }

    private static class StubRaftLogAppendIndexMonitor implements RaftLogAppendIndexMonitor
    {
        private long appendIndex;

        @Override
        public long appendIndex()
        {
            return appendIndex;
        }

        @Override
        public void appendIndex( long appendIndex )
        {
            this.appendIndex = appendIndex;
        }
    }
}
