/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
@ExtendWith( LifeExtension.class )
class SegmentedRaftLogRotationTest
{
    private static final int ROTATE_AT_SIZE_IN_BYTES = 100;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private LifeSupport life;

    @Test
    void shouldRotateOnAppendWhenRotateSizeIsReached() throws Exception
    {
        // When
        SegmentedRaftLog log = life.add( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );
        log.append( new RaftLogEntry( 0, replicatedStringOfBytes( ROTATE_AT_SIZE_IN_BYTES ) ) );

        // Then
        File[] files = fileSystem.listFiles( testDirectory.homeDir(), ( dir, name ) -> name.startsWith( "raft" ) );
        assertEquals( 2, files.length );
    }

    @Test
    void shouldBeAbleToRecoverToLatestStateAfterRotation() throws Throwable
    {
        // Given
        int term = 0;
        long indexToRestoreTo;
        try ( Lifespan lifespan = new Lifespan() )
        {
            SegmentedRaftLog log = lifespan.add( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );
            log.append( new RaftLogEntry( term, replicatedStringOfBytes( ROTATE_AT_SIZE_IN_BYTES - 40 ) ) );
            indexToRestoreTo = log.append( new RaftLogEntry( term, ReplicatedInteger.valueOf( 1 ) ) );
        }

        // When
        SegmentedRaftLog log = life.add( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );

        // Then
        assertEquals( indexToRestoreTo, log.appendIndex() );
        assertEquals( term, log.readEntryTerm( indexToRestoreTo ) );
    }

    private static ReplicatedString replicatedStringOfBytes( int size )
    {
        return new ReplicatedString( "i".repeat( size ) );
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( raft_log_pruning_strategy.defaultValue(), logProvider )
                        .newInstance();
        return new SegmentedRaftLog( fileSystem, testDirectory.homePath(), rotateAtSize,
                ignored -> new DummyRaftableContentSerializer(), logProvider, 0, Clocks.fakeClock(), new OnDemandJobScheduler(),
                pruningStrategy, INSTANCE );
    }
}
