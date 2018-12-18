/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogContractTest;
import com.neo4j.causalclustering.core.state.CoreStateFiles;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.rule.LifeRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.time.Clocks;

import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogContractTest extends RaftLogContractTest
{
    private final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final LifeRule life = new LifeRule( true );

    @Rule
    public RuleChain chain = RuleChain.outerRule( fsRule ).around( life );

    @Override
    public RaftLog createRaftLog()
    {
        File directory = new File( CoreStateFiles.RAFT_LOG.directoryName() );
        FileSystemAbstraction fileSystem = fsRule.get();
        fileSystem.mkdir( directory );

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( "1 entries", logProvider ).newInstance();
        return life.add( new SegmentedRaftLog( fileSystem, directory, 1024, ignored -> new DummyRaftableContentSerializer(),
                logProvider, 8, Clocks.fakeClock(), new OnDemandJobScheduler(), pruningStrategy ) );
    }
}
