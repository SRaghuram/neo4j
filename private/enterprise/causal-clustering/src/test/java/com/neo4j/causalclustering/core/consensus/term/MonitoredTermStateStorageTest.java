/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.term;

import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftTermMonitor;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.monitoring.Monitors;

class MonitoredTermStateStorageTest
{
    @Test
    void shouldMonitorTerm() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        StubRaftTermMonitor raftTermMonitor = new StubRaftTermMonitor();
        monitors.addMonitorListener( raftTermMonitor );
        TermState state = new TermState();
        MonitoredTermStateStorage monitoredTermStateStorage =
                new MonitoredTermStateStorage( new InMemoryStateStorage<>( new TermState() ), monitors );

        // when
        state.update( 7 );
        monitoredTermStateStorage.writeState( state );

        // then
        Assertions.assertEquals( 7, raftTermMonitor.term() );
    }

    private static class StubRaftTermMonitor implements RaftTermMonitor
    {
        private long term;

        @Override
        public long term()
        {
            return term;
        }

        @Override
        public void term( long term )
        {
            this.term = term;
        }
    }
}
