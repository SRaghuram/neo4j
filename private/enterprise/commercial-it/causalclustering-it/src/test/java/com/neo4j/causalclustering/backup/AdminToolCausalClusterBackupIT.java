/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.backup;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
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
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.runBackupToolFromSameJvm;
import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.SHARED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.metrics.MetricsSettings.metricsEnabled;

@ExtendWith( {SuppressOutputExtension.class, TestDirectoryExtension.class} )
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
                .withNumberOfReadReplicas( 0 )
                .withDiscoveryServiceType( SHARED );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldAllowBackupWithTimestampAsDirectoryName() throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader();

        File backupDir = testDirectory.directory( "backups", newBackupDirName() );
        String backupAddress = leader.config().get( OnlineBackupSettings.online_backup_listen_address ).toString();

        cluster.coreTx( this::createSomeData );

        int exitCode = runBackupToolFromSameJvm( testDir,
                "--from=" + backupAddress,
                "--backup-dir=" + backupDir,
                "--database=" + DEFAULT_DATABASE_NAME );

        assertEquals( 0, exitCode );

        DbRepresentation leaderDbRepresentation = DbRepresentation.of( leader.database() );
        DbRepresentation backupDbRepresentation = DbRepresentation.of( new File( backupDir, DEFAULT_DATABASE_NAME ), tempDbConfig() );
        assertEquals( leaderDbRepresentation, backupDbRepresentation );
    }

    private void createSomeData( GraphDatabaseService db, Transaction tx )
    {
        Node node1 = db.createNode( label( "Person" ) );
        node1.setProperty( "id", 1 );

        Node node2 = db.createNode( label( "Person" ) );
        node2.setProperty( "id", 2 );

        node1.createRelationshipTo( node2, withName( "KNOWS" ) );
        tx.success();
    }

    private static Config tempDbConfig()
    {
        return Config.builder()
                .withSetting( metricsEnabled, FALSE )
                .build();
    }

    private static String newBackupDirName()
    {
        String name = LocalDateTime.now().toString();
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            // ':' is an illegal file name character on Windows, so turn it into '_'
            name = name.replaceAll( ":", "_" );
        }
        return name;
    }
}
