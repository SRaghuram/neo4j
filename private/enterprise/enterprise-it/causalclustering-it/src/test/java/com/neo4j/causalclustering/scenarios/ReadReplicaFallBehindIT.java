/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.readreplica.CatchupPollingProcess;
import com.neo4j.causalclustering.readreplica.CatchupProcessManager;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.forceTxLogRotationAndCheckpoint;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
class ReadReplicaFallBehindIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( keep_logical_logs, FALSE )
            .withNumberOfReadReplicas( 1 );

    @BeforeAll
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldReconcileCopiedStore() throws Exception
    {
        ReadReplica readReplica = cluster.getReadReplicaByIndex( 0 );
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, Set.of( readReplica ) );

        CatchupProcessManager catchupProcessManager = readReplica.resolveDependency( SYSTEM_DATABASE_NAME, CatchupProcessManager.class );
        catchupProcessManager.stop();

        // we need to create a few databases (causing a few transactions) so that the log pruning actually happens
        List<String> databaseNames = List.of( "foo", "bar", "baz" );

        for ( String databaseName : databaseNames )
        {
            createDatabase( databaseName, cluster );
            for ( CoreClusterMember core : cluster.coreMembers() )
            {
                forceTxLogRotationAndCheckpoint( core.database( SYSTEM_DATABASE_NAME ) );
            }
            assertDatabaseEventuallyStarted( databaseName, cluster.coreMembers() );
        }

        // none of the databases should exist yet on the read replica
        for ( String databaseName : databaseNames )
        {
            assertThrows( DatabaseNotFoundException.class, () -> readReplica.database( databaseName ) );
        }

        // this should make the read replica start pulling again and realise it needs a store copy of system database
        catchupProcessManager.start();
        CatchupPollingProcess catchupProcess = catchupProcessManager.getCatchupProcess();

        // this will be true after the store copy, when we are back to pulling transactions normally again
        assertTrue( catchupProcess.upToDateFuture().get( 1, MINUTES ) );

        for ( String databaseName : databaseNames )
        {
            // and this can only happen if the internal components got notified after the store copy
            assertDatabaseEventuallyStarted( databaseName, Set.of( readReplica ) );
        }

        // also check that the tracker of reconciled transaction IDs actually got updated as well
        TransactionIdStore txIdStore = readReplica.resolveDependency( SYSTEM_DATABASE_NAME, TransactionIdStore.class );
        long lastClosedTransactionId = txIdStore.getLastClosedTransactionId();
        ReconciledTransactionTracker reconciledTxTracker = readReplica.resolveDependency( SYSTEM_DATABASE_NAME, ReconciledTransactionTracker.class );
        assertEventually( reconciledTxTracker::getLastReconciledTransactionId, equalityCondition( lastClosedTransactionId ), 60, SECONDS );
    }
}
