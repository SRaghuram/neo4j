/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogContractTest;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralTestDirectoryExtension
@ExtendWith( LifeExtension.class )
public class SegmentedRaftLogContractTest extends RaftLogContractTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private LifeSupport life;

    @Override
    public RaftLog createRaftLog()
    {
        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( "1 entries", logProvider ).newInstance();
        return life.add( new SegmentedRaftLog( testDirectory.getFileSystem(), testDirectory.homePath(), 1024, ignored -> new DummyRaftableContentSerializer(),
                logProvider, 8, Clocks.fakeClock(), new OnDemandJobScheduler(), pruningStrategy, INSTANCE ) );
    }
}
