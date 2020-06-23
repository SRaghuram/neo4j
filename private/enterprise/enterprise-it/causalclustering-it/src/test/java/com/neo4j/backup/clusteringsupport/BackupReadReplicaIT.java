/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.backupArguments;
import static com.neo4j.backup.BackupTestUtil.createSomeData;
import static com.neo4j.backup.BackupTestUtil.runBackupToolFromOtherJvmToGetExitCode;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.backupAddress;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.function.Predicates.awaitEx;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class BackupReadReplicaIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;
    private Path backupsDir;

    @BeforeEach
    void setup() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withSharedCoreParam( online_backup_enabled, FALSE )
                .withNumberOfReadReplicas( 1 )
                .withSharedReadReplicaParam( online_backup_enabled, TRUE );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        backupsDir = testDirectory.cleanDirectory( "backups" ).toPath();
    }

    @Test
    void makeSureBackupCanBePerformed() throws Throwable
    {
        // Run backup
        GraphDatabaseFacade leader = createSomeData( cluster );

        GraphDatabaseFacade readReplica = cluster.findAnyReadReplica().defaultDatabase();

        awaitEx( () -> readReplicasUpToDateAsTheLeader( leader, readReplica ), 1, TimeUnit.MINUTES );

        DbRepresentation beforeChange = DbRepresentation.of( readReplica );
        String backupAddress = backupAddress( readReplica );

        String[] args = backupArguments( backupAddress, backupsDir, DEFAULT_DATABASE_NAME );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( readReplica.databaseLayout().databaseDirectory(), args ) );

        // Add some new data
        DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

        // Verify that backed up database can be started and compare representation
        DbRepresentation backupRepresentation = DbRepresentation.of( DatabaseLayout.ofFlat( backupsDir.resolve( DEFAULT_DATABASE_NAME ) ) );
        assertEquals( beforeChange, backupRepresentation );
        assertNotEquals( backupRepresentation, afterChange );
    }

    private static boolean readReplicasUpToDateAsTheLeader( GraphDatabaseFacade leader, GraphDatabaseFacade readReplica )
    {
        long leaderTxId = leader.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastClosedTransactionId();
        long lastClosedTxId = readReplica.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastClosedTransactionId();
        return lastClosedTxId == leaderTxId;
    }
}
