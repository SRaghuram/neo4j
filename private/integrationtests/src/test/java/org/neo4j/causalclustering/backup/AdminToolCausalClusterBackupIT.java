/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.backup;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.time.LocalDateTime;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static com.neo4j.util.TestHelpers.runBackupToolFromSameJvm;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.metrics.MetricsSettings.metricsEnabled;

public class AdminToolCausalClusterBackupIT
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    private Cluster<?> cluster;
    private File testDir;
    private File backupsDir;

    @Before
    public void setup() throws Exception
    {
        testDir = clusterRule.testDirectory().absolutePath();
        backupsDir = clusterRule.testDirectory().cleanDirectory( "backups" );
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldAllowBackupWithTimestampAsName() throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader();

        String backupName = LocalDateTime.now().toString();
        String backupAddress = leader.config().get( OnlineBackupSettings.online_backup_listen_address ).toString();

        cluster.coreTx( this::createSomeData );

        int exitCode = runBackupToolFromSameJvm( testDir,
                "--from=" + backupAddress,
                "--backup-dir=" + backupsDir,
                "--name=" + backupName );

        assertEquals( 0, exitCode );

        DbRepresentation leaderDbRepresentation = DbRepresentation.of( leader.database() );
        DbRepresentation backupDbRepresentation = DbRepresentation.of( new File( backupsDir, backupName ), tempDbConfig() );
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
}
