/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.backup;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.time.LocalDateTime;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.runBackupToolFromSameJvm;
import static com.neo4j.configuration.MetricsSettings.metrics_enabled;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
class AdminToolCausalClusterBackupIT
{
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private TestDirectory testDirectory;

    private Cluster cluster;
    private File testDir;

    @BeforeEach
    void setup() throws Exception
    {
        testDir = testDirectory.absolutePath();

        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldAllowBackupWithTimestampAsDirectoryName() throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader();

        File backupDir = testDirectory.directory( "backups", newBackupDirName() );
        String backupAddress = leader.config().get( OnlineBackupSettings.online_backup_listen_address ).toString();

        cluster.coreTx( AdminToolCausalClusterBackupIT::createSomeData );

        int exitCode = runBackupToolFromSameJvm( testDir,
                "--from=" + backupAddress,
                "--backup-dir=" + backupDir,
                "--database=" + DEFAULT_DATABASE_NAME );

        assertEquals( 0, exitCode );

        DbRepresentation leaderDbRepresentation = DbRepresentation.of( leader.defaultDatabase() );
        DbRepresentation backupDbRepresentation = DbRepresentation.of( DatabaseLayout.ofFlat( new File( backupDir, DEFAULT_DATABASE_NAME ) ), tempDbConfig() );
        assertEquals( leaderDbRepresentation, backupDbRepresentation );
    }

    private static void createSomeData( GraphDatabaseService db, Transaction tx )
    {
        Node node1 = tx.createNode( label( "Person" ) );
        node1.setProperty( "id", 1 );

        Node node2 = tx.createNode( label( "Person" ) );
        node2.setProperty( "id", 2 );

        node1.createRelationshipTo( node2, withName( "KNOWS" ) );
        tx.commit();
    }

    private static Config tempDbConfig()
    {
        return Config.defaults( metrics_enabled, false );
    }

    private static String newBackupDirName()
    {
        String name = LocalDateTime.now().toString();
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            // ':' is an illegal file name character on Windows, so turn it into '_'
            name = name.replace( ':', '_' );
        }
        return name;
    }
}
