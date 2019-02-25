/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.helpers.SampleData;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CorePruningIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withSharedCoreParam( CausalClusteringSettings.state_machine_flush_window_size, "1" )
                    .withSharedCoreParam( raft_log_pruning_strategy, "keep_none" )
                    .withSharedCoreParam( CausalClusteringSettings.raft_log_rotation_size, "1K" )
                    .withSharedCoreParam( CausalClusteringSettings.raft_log_pruning_frequency, "100ms" );

    @Test
    public void actuallyDeletesTheFiles() throws Exception
    {
        // given
        Cluster<?> cluster = clusterRule.startCluster();

        CoreClusterMember coreGraphDatabase = null;
        int txs = 10;
        for ( int i = 0; i < txs; i++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) ->
            {
                SampleData.createData( db, 1 );
                tx.success();
            } );
        }

        // when pruning kicks in then some files are actually deleted
        File raftLogDir = coreGraphDatabase.raftLogDirectory();
        int expectedNumberOfLogFilesAfterPruning = 2;
        assertEventually( "raft logs eventually pruned", () -> numberOfFiles( raftLogDir ),
                equalTo( expectedNumberOfLogFilesAfterPruning ), 5, TimeUnit.SECONDS );
    }

    @Test
    public void shouldNotPruneUncommittedEntries() throws Exception
    {
        // given
        Cluster<?> cluster = clusterRule.startCluster();

        CoreClusterMember coreGraphDatabase = null;
        int txs = 1000;
        for ( int i = 0; i < txs; i++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) -> SampleData.createData( db, 1 ) );
        }

        // when pruning kicks in then some files are actually deleted
        int expectedNumberOfLogFilesAfterPruning = 2;
        File raftLogDir = coreGraphDatabase.raftLogDirectory();
        assertEventually( "raft logs eventually pruned", () -> numberOfFiles( raftLogDir ),
                equalTo( expectedNumberOfLogFilesAfterPruning ), 5, TimeUnit.SECONDS );
    }

    private int numberOfFiles( File raftLogDir ) throws RuntimeException
    {
        return raftLogDir.list().length;
    }
}
