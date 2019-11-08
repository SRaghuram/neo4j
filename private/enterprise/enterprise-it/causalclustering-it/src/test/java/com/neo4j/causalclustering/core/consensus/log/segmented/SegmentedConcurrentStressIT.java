/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.ConcurrentStressIT;
import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;

import java.io.File;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;

public class SegmentedConcurrentStressIT extends ConcurrentStressIT
{
    @Override
    public RaftLog createRaftLog( FileSystemAbstraction fsa, File dir )
    {
        var rotateAtSize = ByteUnit.mebiBytes( 8 );
        var logProvider = NullLogProvider.getInstance();
        var readerPoolSize = 8;
        var pruningStrategy = new CoreLogPruningStrategyFactory( raft_log_pruning_strategy.defaultValue(), logProvider ).newInstance();
        return new SegmentedRaftLog( fsa, dir, rotateAtSize, ignored -> new DummyRaftableContentSerializer(), logProvider,
                readerPoolSize, Clocks.fakeClock(), new OnDemandJobScheduler(), pruningStrategy );
    }
}
