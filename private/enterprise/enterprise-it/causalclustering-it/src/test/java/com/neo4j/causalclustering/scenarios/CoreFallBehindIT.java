/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDiscoveryService;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDiscoveryService;
import static com.neo4j.configuration.CausalClusteringSettings.minimum_core_cluster_size_at_runtime;
import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_frequency;
import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_strategy;
import static com.neo4j.configuration.CausalClusteringSettings.raft_log_rotation_size;
import static com.neo4j.configuration.CausalClusteringSettings.state_machine_flush_window_size;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.FALSE;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
class CoreFallBehindIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( minimum_core_cluster_size_at_runtime, "2" )
            .withSharedCoreParam( state_machine_flush_window_size, "1" )
            .withSharedCoreParam( raft_log_pruning_strategy, "keep_none" )
            .withSharedCoreParam( raft_log_rotation_size, "1K" )
            .withSharedCoreParam( raft_log_pruning_frequency, "100ms" )
            .withNumberOfReadReplicas( 0 );

    static class DownloadMonitor implements PersistentSnapshotDownloader.Monitor
    {
        private AtomicInteger systemDownloadCount = new AtomicInteger();

        @Override
        public void startedDownloadingSnapshot( NamedDatabaseId namedDatabaseId )
        {
        }

        @Override
        public void downloadSnapshotComplete( NamedDatabaseId namedDatabaseId )
        {
            if ( namedDatabaseId.isSystemDatabase() )
            {
                systemDownloadCount.incrementAndGet();
            }
        }

        int systemDownloadCount()
        {
            return systemDownloadCount.get();
        }
    }

    @BeforeAll
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldReconcileCopiedStore() throws Exception
    {
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );
        // given a follower that soon will be made to fall behind
        CoreClusterMember staleFollower = cluster.getMemberWithAnyRole( SYSTEM_DATABASE_NAME, Role.FOLLOWER );

        // make sure this follower gets disconnected from the system Raft, by completely disconnecting it from discovery
        // this will cause the Leader to stop pushing new Raft log entries to that follower, causing it to fall behind
        stopDiscoveryService( staleFollower );
        assertEventually( () -> isSystemDatabaseMember( staleFollower ), FALSE, 60, SECONDS );

        DownloadMonitor downloadMonitor = new DownloadMonitor();
        staleFollower.monitors().addMonitorListener( downloadMonitor );

        // then create a database of which that follower should be unaware
        createDatabase( "foo", cluster );
        Set<CoreClusterMember> remaining = cluster.coreMembers().stream().filter( m -> !m.id().equals( staleFollower.id() ) ).collect( toSet() );
        assertDatabaseEventuallyStarted( "foo", remaining );

        // next make sure it cannot catchup from the others through catching up on Raft log entries, forcing an eventual store copy
        for ( CoreClusterMember core : remaining )
        {
            forceRaftLogRotationAndPruning( core );
        }

        // just an extra assertion making sure a download of the system database hasn't occurred yet
        assertEquals( 0, downloadMonitor.systemDownloadCount() );

        // when we reconnect the follower
        startDiscoveryService( staleFollower );

        // then it should download the system database and see that a new database foo shall exist and create it
        assertEventually( () -> isSystemDatabaseMember( staleFollower ), TRUE, 60, SECONDS );
        assertDatabaseEventuallyStarted( "foo", Set.of( staleFollower ) );
        assertEventually( downloadMonitor::systemDownloadCount, equalityCondition( 1 ), 60, SECONDS );

        // also check that the tracker of reconciled transaction IDs actually got updated as well
        TransactionIdStore txIdStore = staleFollower.resolveDependency( SYSTEM_DATABASE_NAME, TransactionIdStore.class );
        long lastClosedTransactionId = txIdStore.getLastClosedTransactionId();
        ReconciledTransactionTracker reconciledTxTracker = staleFollower.resolveDependency( SYSTEM_DATABASE_NAME, ReconciledTransactionTracker.class );
        assertEventually( reconciledTxTracker::getLastReconciledTransactionId, equalityCondition( lastClosedTransactionId ), 60, SECONDS );
    }

    private void forceRaftLogRotationAndPruning( CoreClusterMember core ) throws Exception
    {
        SortedMap<Long,File> sortedRaftLogs = core.getRaftLogFileNames( SYSTEM_DATABASE_NAME );
        File lastRaftLog = sortedRaftLogs.get( sortedRaftLogs.lastKey() );

        while ( lastRaftLog.exists() )
        {
            cluster.coreTx( SYSTEM_DATABASE_NAME, ( db, tx ) ->
            {
                tx.createNode();
                tx.commit();
            } );
        }
    }

    private boolean isSystemDatabaseMember( CoreClusterMember core ) throws TimeoutException
    {
        RaftMachine raft = cluster.awaitLeader( SYSTEM_DATABASE_NAME ).resolveDependency( SYSTEM_DATABASE_NAME, RaftMachine.class );
        return raft.votingMembers().contains( core.id() );
    }
}
