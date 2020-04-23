/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
class SegmentedRaftLogRotationTruncationPruneTest
{
    @Inject
    private TestDirectory testDirectory;

    private SegmentedRaftLog log;

    @BeforeEach
    void before() throws Exception
    {
        log = createRaftLog();
    }

    @AfterEach
    void after() throws Exception
    {
        log.stop();
    }

    @Test
    void shouldPruneAwaySingleEntriesIfRotationHappenedEveryEntry() throws Exception
    {
        /*
         * If you have a raft log which rotates after every append, therefore having a single entry in every segment,
         * we assert that every sequential prune attempt will result in the prevIndex incrementing by one.
         */

        long term = 0;
        for ( int i = 0; i < 10; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }

        assertEquals( -1, log.prevIndex() );
        for ( int i = 0; i < 9; i++ )
        {
            log.prune( i );
            assertEquals( i, log.prevIndex() );
        }
        log.prune( 9 );
        assertEquals( 8, log.prevIndex() );
    }

    @Test
    void shouldPruneAwaySingleEntriesAfterTruncationIfRotationHappenedEveryEntry() throws Exception
    {
        /*
         * Given a log with many single-entry segments, a series of truncations at descending values followed by
         * pruning at more previous segments will maintain the correct prevIndex for the log.
         *
         * Initial Scenario:    [0][1][2][3][4][5][6][7][8][9]              prevIndex = 0
         * Truncate segment 9 : [0][1][2][3][4][5][6][7][8]   (9)           prevIndex = 0
         * Truncate segment 8 : [0][1][2][3][4][5][6][7]      (8)(9)        prevIndex = 0
         * Truncate segment 7 : [0][1][2][3][4][5][6]         (7)(8)(9)     prevIndex = 0
         * Prune segment 0    :    [1][2][3][4][5][6]         (7)(8)(9)     prevIndex = 1
         * Prune segment 1    :       [2][3][4][5][6]         (7)(8)(9)     prevIndex = 2
         * Prune segment 2    :          [3][4][5][6]         (7)(8)(9)     prevIndex = 3
         * Prune segment 3    :             [4][5][6]         (7)(8)(9)     prevIndex = 4
         * Prune segment 4    :                [5][6]         (7)(8)(9)     prevIndex = 5
         * Prune segment 5    :                [5][6]         (7)(8)(9)     prevIndex = 5
         * Prune segment 6    :                [5][6]         (7)(8)(9)     prevIndex = 5
         * Etc...
         *
         * The prevIndex should not become corrupt and become greater than 5 in this example.
         */

        long term = 0;
        for ( int i = 0; i < 10; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }

        log.truncate( 9 );
        log.truncate( 8 );
        log.truncate( 7 );

        assertEquals( -1, log.prevIndex() );
        for ( int i = 0; i <= 5; i++ )
        {
            log.prune( i );
            assertEquals( i, log.prevIndex() );
        }
        for ( int i = 5; i < 10; i++ )
        {
            log.prune( i );
            assertEquals( 5, log.prevIndex() );
        }
    }

    private SegmentedRaftLog createRaftLog() throws Exception
    {
        File directory = testDirectory.homeDir();

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( "1 entries", logProvider ).newInstance();

        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( testDirectory.getFileSystem(), directory, 1,
                ignored -> new DummyRaftableContentSerializer(), logProvider, 8, Clocks.fakeClock(), new OnDemandJobScheduler(),
                pruningStrategy, INSTANCE );

        newRaftLog.start();
        return newRaftLog;
    }
}
