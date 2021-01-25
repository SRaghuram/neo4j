/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.outcome;

import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.RaftLogHelper;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;

class BatchAppendLogEntriesTest
{
    private final Log log = NullLog.getInstance();
    private RaftLogEntry entryA = new RaftLogEntry( 0, valueOf( 100 ) );
    private RaftLogEntry entryB = new RaftLogEntry( 1, valueOf( 200 ) );
    private RaftLogEntry entryC = new RaftLogEntry( 2, valueOf( 300 ) );
    private RaftLogEntry entryD = new RaftLogEntry( 3, valueOf( 400 ) );

    @Test
    void shouldApplyMultipleEntries() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        BatchAppendLogEntries batchAppendLogEntries =
                new BatchAppendLogEntries( 0, 0, new RaftLogEntry[]{entryA, entryB, entryC} );

        // when
        batchAppendLogEntries.applyTo( raftLog, log );

        // then
        Assertions.assertEquals( entryA, RaftLogHelper.readLogEntry( raftLog, 0 ) );
        Assertions.assertEquals( entryB, RaftLogHelper.readLogEntry( raftLog, 1 ) );
        Assertions.assertEquals( entryC, RaftLogHelper.readLogEntry( raftLog, 2 ) );
        Assertions.assertEquals( 2, raftLog.appendIndex() );
    }

    @Test
    void shouldApplyFromOffsetOnly() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        BatchAppendLogEntries batchAppendLogEntries =
                new BatchAppendLogEntries( 0, 2, new RaftLogEntry[]{entryA, entryB, entryC, entryD} );

        // when
        batchAppendLogEntries.applyTo( raftLog, log );

        // then
        Assertions.assertEquals( entryC, RaftLogHelper.readLogEntry( raftLog, 0 ) );
        Assertions.assertEquals( entryD, RaftLogHelper.readLogEntry( raftLog, 1 ) );
        Assertions.assertEquals( 1, raftLog.appendIndex() );
    }

    @Test
    void applyTo()
    {
        //Test that batch commands apply entries to the cache.

        //given
        long baseIndex = 0;
        int offset = 1;
        RaftLogEntry[] entries =
                new RaftLogEntry[]{new RaftLogEntry( 0L, valueOf( 0 ) ), new RaftLogEntry( 1L, valueOf( 1 ) ),
                        new RaftLogEntry( 2L, valueOf( 2 ) ),};

        BatchAppendLogEntries batchAppend = new BatchAppendLogEntries( baseIndex, offset, entries );

        InFlightCache cache = new ConsecutiveInFlightCache();

        //when
        batchAppend.applyTo( cache, log );

        //then
        Assertions.assertNull( cache.get( 0L ) );
        Assertions.assertNotNull( cache.get( 1L ) );
        Assertions.assertNotNull( cache.get( 2L ) );
    }
}
