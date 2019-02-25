/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.SuppressOutput;

import static com.neo4j.causalclustering.helpers.CausalClusteringTestHelpers.transactionAddress;
import static com.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.backup.clusteringsupport.BackupUtil.getConfig;
import static org.neo4j.function.Predicates.awaitEx;

public class BackupReadReplicaIT
{
    @Rule
    public SuppressOutput suppress = SuppressOutput.suppressAll();

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withNumberOfReadReplicas( 1 )
            .withSharedReadReplicaParam( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );

    private Cluster<?> cluster;
    private File backupPath;

    @Before
    public void setup() throws Exception
    {
        backupPath = clusterRule.testDirectory().cleanDirectory( "backup-db" );
        cluster = clusterRule.startCluster();
    }

    @Test
    public void makeSureBackupCanBePerformed() throws Throwable
    {
        // Run backup
        CoreGraphDatabase leader = BackupUtil.createSomeData( cluster );

        ReadReplicaGraphDatabase readReplica = cluster.findAnyReadReplica().database();

        awaitEx( () -> readReplicasUpToDateAsTheLeader( leader, readReplica ), 1, TimeUnit.MINUTES );

        DbRepresentation beforeChange = DbRepresentation.of( readReplica );
        String backupAddress = transactionAddress( readReplica );

        String[] args = BackupUtil.backupArguments( backupAddress, backupPath, "readreplica" );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( clusterRule.clusterDirectory(), args ) );

        // Add some new data
        DbRepresentation afterChange = DbRepresentation.of( BackupUtil.createSomeData( cluster ) );

        // Verify that backed up database can be started and compare representation
        DbRepresentation backupRepresentation = DbRepresentation.of( DatabaseLayout.of( backupPath, "readreplica" ).databaseDirectory(), getConfig() );
        assertEquals( beforeChange, backupRepresentation );
        assertNotEquals( backupRepresentation, afterChange );
    }

    private static boolean readReplicasUpToDateAsTheLeader( CoreGraphDatabase leader, ReadReplicaGraphDatabase readReplica )
    {
        long leaderTxId = leader.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastClosedTransactionId();
        long lastClosedTxId = readReplica.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastClosedTransactionId();
        return lastClosedTxId == leaderTxId;
    }
}
