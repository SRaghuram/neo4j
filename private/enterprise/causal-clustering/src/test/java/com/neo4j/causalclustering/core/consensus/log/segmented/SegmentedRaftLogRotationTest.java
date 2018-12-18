/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.rule.LifeRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.junit.Assert.assertEquals;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogRotationTest
{
    private static final int ROTATE_AT_SIZE_IN_BYTES = 100;

    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final LifeRule life = new LifeRule( true );
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                                          .around( fileSystemRule ).around( life );

    @Test
    public void shouldRotateOnAppendWhenRotateSizeIsReached() throws Exception
    {
        // When
        SegmentedRaftLog log = life.add( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );
        log.append( new RaftLogEntry( 0, replicatedStringOfBytes( ROTATE_AT_SIZE_IN_BYTES ) ) );

        // Then
        File[] files = fileSystemRule.get().listFiles( testDirectory.directory(), ( dir, name ) -> name.startsWith( "raft" ) );
        assertEquals( 2, files.length );
    }

    @Test
    public void shouldBeAbleToRecoverToLatestStateAfterRotation() throws Throwable
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
        return new ReplicatedString( StringUtils.repeat( "i", size ) );
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( raft_log_pruning_strategy.getDefaultValue(), logProvider )
                        .newInstance();
        return new SegmentedRaftLog( fileSystemRule.get(), testDirectory.directory(), rotateAtSize,
                ignored -> new DummyRaftableContentSerializer(), logProvider, 0, Clocks.fakeClock(), new OnDemandJobScheduler(),
                pruningStrategy );
    }
}
