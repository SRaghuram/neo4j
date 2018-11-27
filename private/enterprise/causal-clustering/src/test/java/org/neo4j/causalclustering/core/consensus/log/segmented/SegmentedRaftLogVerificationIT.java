/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.File;

import org.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogVerificationIT;
import org.neo4j.causalclustering.core.state.CoreStateFiles;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogVerificationIT extends RaftLogVerificationIT
{
    @Override
    protected RaftLog createRaftLog() throws Throwable
    {
        FileSystemAbstraction fsa = fsRule.get();

        File directory = new File( CoreStateFiles.RAFT_LOG.directoryName() );
        fsa.mkdir( directory );

        long rotateAtSizeBytes = 128;
        int readerPoolSize = 8;

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( raft_log_pruning_strategy.getDefaultValue(), logProvider )
                        .newInstance();
        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( fsa, directory, rotateAtSizeBytes,
                ignored -> new DummyRaftableContentSerializer(), logProvider, readerPoolSize, Clocks.systemClock(),
                new OnDemandJobScheduler(),
                pruningStrategy );

        newRaftLog.init();
        newRaftLog.start();

        return newRaftLog;
    }

    @Override
    protected long operations()
    {
        return 500;
    }
}
