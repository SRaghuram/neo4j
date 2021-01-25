/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
@ExtendWith( LifeExtension.class )
class SegmentedRaftLogCursorIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private LifeSupport life;

    @Test
    void shouldReturnFalseOnCursorForEntryThatDoesntExist() throws Exception
    {
        //given
        SegmentedRaftLog segmentedRaftLog = createRaftLog( 1 );
        segmentedRaftLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );
        segmentedRaftLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 2 ) ) );
        long lastIndex = segmentedRaftLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 3 ) ) );

        //when
        boolean next;
        try ( RaftLogCursor entryCursor = segmentedRaftLog.getEntryCursor( lastIndex + 1 ) )
        {
            next = entryCursor.next();
        }

        //then
        assertFalse( next );
    }

    @Test
    void shouldReturnTrueOnEntryThatExists() throws Exception
    {
        //given
        SegmentedRaftLog segmentedRaftLog = createRaftLog( 1 );
        segmentedRaftLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );
        segmentedRaftLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 2 ) ) );
        long lastIndex = segmentedRaftLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 3 ) ) );

        //when
        boolean next;
        try ( RaftLogCursor entryCursor = segmentedRaftLog.getEntryCursor( lastIndex ) )
        {
            next = entryCursor.next();
        }

        //then
        assertTrue( next );
    }

    @Test
    void shouldReturnFalseOnCursorForEntryThatWasPruned() throws Exception
    {
        //given
        SegmentedRaftLog segmentedRaftLog = createRaftLog( 1, "keep_none" );
        long firstIndex = segmentedRaftLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );
        segmentedRaftLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 2 ) ) );
        long lastIndex = segmentedRaftLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 3 ) ) );

        //when
        segmentedRaftLog.prune( firstIndex );
        RaftLogCursor entryCursor = segmentedRaftLog.getEntryCursor( firstIndex );
        boolean next = entryCursor.next();

        //then
        assertFalse( next );
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        return createRaftLog( rotateAtSize, raft_log_pruning_strategy.defaultValue() );
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize, String pruneStrategy )
    {
        Path directory = testDirectory.homePath();

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( pruneStrategy, logProvider ).newInstance();
        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( testDirectory.getFileSystem(), directory, rotateAtSize,
                ignored -> new DummyRaftableContentSerializer(), logProvider, 8, Clocks.systemClock(),
                new OnDemandJobScheduler(), pruningStrategy, INSTANCE );

        life.add( newRaftLog );

        return newRaftLog;
    }
}
