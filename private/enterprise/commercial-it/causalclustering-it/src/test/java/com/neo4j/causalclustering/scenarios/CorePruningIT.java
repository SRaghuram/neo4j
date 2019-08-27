/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_frequency;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_rotation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_flush_window_size;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@ExtendWith( DefaultFileSystemExtension.class )
@TestInstance( PER_METHOD )
class CorePruningIT
{
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private FileSystemAbstraction fs;

    private Cluster cluster;

    @BeforeEach
    void beforeEach() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( state_machine_flush_window_size, "1" )
                .withSharedCoreParam( raft_log_pruning_strategy, "keep_none" )
                .withSharedCoreParam( raft_log_rotation_size, "1K" )
                .withSharedCoreParam( raft_log_pruning_frequency, "100ms" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void actuallyDeletesTheFiles() throws Exception
    {
        CoreClusterMember coreGraphDatabase = null;
        int txs = 10;
        for ( int i = 0; i < txs; i++ )
        {
            coreGraphDatabase = DataCreator.createDataInOneTransaction( cluster, 1 );
        }

        // when pruning kicks in then some files are actually deleted
        File raftLogDir = coreGraphDatabase.raftLogDirectory( DEFAULT_DATABASE_NAME );
        int expectedNumberOfLogFilesAfterPruning = 2;
        assertEventually( "raft logs eventually pruned", () -> numberOfFiles( raftLogDir ),
                equalTo( expectedNumberOfLogFilesAfterPruning ), 5, TimeUnit.SECONDS );
    }

    @Test
    void shouldNotPruneUncommittedEntries() throws Exception
    {
        CoreClusterMember coreGraphDatabase = null;
        int txs = 1000;
        for ( int i = 0; i < txs; i++ )
        {
            coreGraphDatabase = DataCreator.createDataInOneTransaction( cluster, 1 );
        }

        // when pruning kicks in then some files are actually deleted
        int expectedNumberOfLogFilesAfterPruning = 2;
        File raftLogDir = coreGraphDatabase.raftLogDirectory( DEFAULT_DATABASE_NAME );
        assertEventually( "raft logs eventually pruned", () -> numberOfFiles( raftLogDir ),
                equalTo( expectedNumberOfLogFilesAfterPruning ), 5, TimeUnit.SECONDS );
    }

    private int numberOfFiles( File raftLogDir ) throws RuntimeException
    {
        return fs.listFiles( raftLogDir ).length;
    }
}
