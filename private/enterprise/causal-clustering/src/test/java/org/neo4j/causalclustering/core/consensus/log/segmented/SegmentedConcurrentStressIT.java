/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.File;

import org.neo4j.causalclustering.core.consensus.log.ConcurrentStressIT;
import org.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedConcurrentStressIT extends ConcurrentStressIT<SegmentedRaftLog>
{
    @Override
    public SegmentedRaftLog createRaftLog( FileSystemAbstraction fsa, File dir )
    {
        long rotateAtSize = ByteUnit.mebiBytes( 8 );
        LogProvider logProvider = getInstance();
        int readerPoolSize = 8;
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( raft_log_pruning_strategy.getDefaultValue(), logProvider )
                        .newInstance();
        return new SegmentedRaftLog( fsa, dir, rotateAtSize, ignored -> new DummyRaftableContentSerializer(), logProvider,
                readerPoolSize, Clocks.fakeClock(), new OnDemandJobScheduler(), pruningStrategy );
    }
}
