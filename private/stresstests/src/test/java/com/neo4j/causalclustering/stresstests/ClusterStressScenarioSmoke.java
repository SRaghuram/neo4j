/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;

import static com.neo4j.causalclustering.stresstests.ClusterStressTesting.stressTest;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.Exceptions.findCauseOrSuppressed;

@ExtendWith( {DefaultFileSystemExtension.class, PageCacheSupportExtension.class} )
class ClusterStressScenarioSmoke
{
    private Config config;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    @BeforeEach
    void setup()
    {
        config = new Config();
        config.workDurationMinutes( 1 );
    }

    @Test
    void stressBackupRandomMemberAndStartStop() throws Exception
    {
        config.workloads( Workloads.CreateNodesWithProperties, Workloads.BackupRandomMember, Workloads.StartStopRandomMember );
        stressTest( config, fileSystem, pageCache );
    }

    @Test
    void stressCatchupNewReadReplica() throws Exception
    {
        config.workloads( Workloads.CreateNodesWithProperties, Workloads.CatchupNewReadReplica, Workloads.StartStopRandomCore );
        stressTest( config, fileSystem, pageCache );
    }

    @Test
    void stressReplaceRandomMember() throws Exception
    {
        config.workloads( Workloads.CreateNodesWithProperties, Workloads.ReplaceRandomMember );
        stressTest( config, fileSystem, pageCache );
    }

    @Test
    void stressStartStopLeader() throws Exception
    {
        config.workloads( Workloads.CreateNodesWithProperties, Workloads.StartStopDefaultDatabaseLeader );
        stressTest( config, fileSystem, pageCache );
    }

    @Test
    void simulateFailure() throws Exception
    {
        try
        {
            config.workloads( Workloads.FailingWorkload, Workloads.StartStopRandomCore );
            stressTest( config, fileSystem, pageCache );
            fail( "Should throw" );
        }
        catch ( RuntimeException rte )
        {
            assertTrue( findCauseOrSuppressed( rte, e -> e.getMessage().equals( FailingWorkload.MESSAGE ) ).isPresent() );
        }
    }

    @Test
    void stressIdReuse() throws Exception
    {
        config.numberOfEdges( 0 );
        config.reelectIntervalSeconds( 20 );

        config.preparations( Preparations.IdReuseSetup );

        // having two deletion workers is on purpose
        config.workloads( Workloads.IdReuseInsertion, Workloads.IdReuseDeletion, Workloads.IdReuseDeletion, Workloads.IdReuseReelection );
        config.validations( Validations.ConsistencyCheck, Validations.IdReuseUniqueFreeIds );

        stressTest( config, fileSystem, pageCache );
    }

    @Test
    void stressCreateManyDatabases() throws Exception
    {
        config.numberOfDatabases( 10 );
        config.workloads( Workloads.CreateManyDatabases );

        stressTest( config, fileSystem, pageCache );
    }

    @Test
    public void stressVmPauseDuringBecomingLeader() throws Exception
    {
        config.numberOfEdges( 0 );
        config.numberOfDatabases( 5 );
        config.workDurationMinutes( 5 );
        config.workloads( Workloads.VmPauseDuringBecomingLeader );

        stressTest( config, fileSystem, pageCache );
    }
}
