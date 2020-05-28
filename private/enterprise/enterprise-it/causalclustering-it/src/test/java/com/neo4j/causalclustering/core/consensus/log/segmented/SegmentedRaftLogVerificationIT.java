/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogVerificationIT;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@ExtendWith( LifeExtension.class )
public class SegmentedRaftLogVerificationIT extends RaftLogVerificationIT
{
    @Inject
    private LifeSupport life;

    @Override
    protected RaftLog createRaftLog( TestDirectory testDirectory )
    {
        File directory = testDirectory.homeDir();

        long rotateAtSizeBytes = 128;
        int readerPoolSize = 8;

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( raft_log_pruning_strategy.defaultValue(), logProvider )
                        .newInstance();
        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( testDirectory.getFileSystem(), directory, rotateAtSizeBytes,
                ignored -> new DummyRaftableContentSerializer(), logProvider, readerPoolSize, Clocks.systemClock(),
                new OnDemandJobScheduler(), pruningStrategy, INSTANCE );

        life.add( newRaftLog );

        return newRaftLog;
    }

    @Override
    protected long operations()
    {
        return 500;
    }
}
