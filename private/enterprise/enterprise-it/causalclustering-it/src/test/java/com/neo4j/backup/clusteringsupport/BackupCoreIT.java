/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.backupArguments;
import static com.neo4j.backup.BackupTestUtil.createSomeData;
import static com.neo4j.backup.BackupTestUtil.runBackupToolFromOtherJvmToGetExitCode;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.backupAddress;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class BackupCoreIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void setup() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void makeSureBackupCanBePerformedFromAnyInstance() throws Throwable
    {
        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            // Run backup
            DbRepresentation beforeChange = DbRepresentation.of( createSomeData( cluster ) );
            Path coreBackupDir = testDirectory.directory( "backups", "core-" + db.serverId() + "-backup" ).toPath();
            String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
            Path coreDefaultDbBackupDir = coreBackupDir.resolve( databaseName );
            String[] args = backupArguments( backupAddress( db.defaultDatabase() ), coreBackupDir, databaseName );
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( db.databaseLayout().databaseDirectory(), args ) );

            // Add some new data
            DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

            // Verify that old data is back
            DbRepresentation backupRepresentation = DbRepresentation.of( DatabaseLayout.ofFlat( coreDefaultDbBackupDir ) );
            assertEquals( beforeChange, backupRepresentation );
            assertNotEquals( backupRepresentation, afterChange );
        }
    }
}
