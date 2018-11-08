/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.After;
import org.junit.Test;

import java.io.File;

import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.state.CoreStateFiles;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogCursorIT
{
    private final LifeSupport life = new LifeSupport();
    private FileSystemAbstraction fileSystem;

    @After
    public void tearDown() throws Throwable
    {
        life.stop();
        life.shutdown();
        fileSystem.close();
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize, String pruneStrategy )
    {
        if ( fileSystem == null )
        {
            fileSystem = new EphemeralFileSystemAbstraction();
        }

        File directory = new File( CoreStateFiles.RAFT_LOG.directoryFullName() );
        fileSystem.mkdir( directory );

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( pruneStrategy, logProvider ).newInstance();
        SegmentedRaftLog newRaftLog =
                new SegmentedRaftLog( fileSystem, directory, rotateAtSize, ignored -> new DummyRaftableContentSerializer(),
                        logProvider, 8, Clocks.systemClock(),
                        new OnDemandJobScheduler(),
                        pruningStrategy );

        life.add( newRaftLog );
        life.init();
        life.start();

        return newRaftLog;
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        return createRaftLog( rotateAtSize, raft_log_pruning_strategy.getDefaultValue() );
    }

    @Test
    public void shouldReturnFalseOnCursorForEntryThatDoesntExist() throws Exception
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
    public void shouldReturnTrueOnEntryThatExists() throws Exception
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
    public void shouldReturnFalseOnCursorForEntryThatWasPruned() throws Exception
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
}
