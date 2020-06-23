/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.backup;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.nio.file.Path;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.runBackupToolFromOtherJvmToGetExitCode;
import static com.neo4j.backup.BackupTestUtil.runBackupToolFromSameJvm;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.backupAddress;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.graphdb.Label.label;

@Neo4jLayoutExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class EnterpriseGraphDatabaseBackupIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private SuppressOutput suppressOutput;
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private DatabaseLayout databaseLayout;

    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldDoBackup() throws Exception
    {
        var nodeCount = 999;
        db = newEnterpriseDb( testDirectory.homePath(), true );
        createNodes( db, nodeCount );

        var backupDir = performBackup();
        managementService.shutdown();

        db = newEnterpriseBackupDb( backupDir, false );
        verifyNodes( nodeCount );

        managementService.shutdown();
    }

    @Test
    void shouldFailWithErrorMessageForUnknownDatabase() throws Exception
    {
        var unknownDbName = "unknowndb";
        db = newEnterpriseDb( testDirectory.homePath(), true );

        var exitCode = runBackupToolFromSameJvm( databaseLayout.databaseDirectory(),
                "--from=" + backupAddress( db ),
                "--backup-dir=" + testDirectory.directory( "backups" ),
                "--database=" + unknownDbName );

        assertEquals( 1, exitCode );

        assertThat( suppressOutput.getErrorVoice().lines(), hasItem( containsString( "Database '" + unknownDbName + "' does not exist" ) ) );
    }

    private Path performBackup() throws Exception
    {
        var storeDir = neo4jLayout.databasesDirectory();
        var backupsDir = testDirectory.directoryPath( "backups" );

        var exitCode = runBackupToolFromOtherJvmToGetExitCode( storeDir,
                "--from=" + backupAddress( db ),
                "--backup-dir=" + backupsDir,
                "--database=" + DEFAULT_DATABASE_NAME );

        assertEquals( 0, exitCode );

        return backupsDir.resolve( DEFAULT_DATABASE_NAME );
    }

    private GraphDatabaseAPI newEnterpriseDb( Path storeDir, boolean backupEnabled )
    {
        managementService = defaultEnterpriseBuilder( storeDir, backupEnabled ).build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private GraphDatabaseAPI newEnterpriseBackupDb( Path databaseDirectory, boolean backupEnabled )
    {
        var storeDir = databaseDirectory.getParent();
        managementService = defaultEnterpriseBuilder( storeDir, backupEnabled )
                .setConfig( transaction_logs_root_path, storeDir.toAbsolutePath() )
                .setConfig( databases_root_path, storeDir.toAbsolutePath() )
                .build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static DatabaseManagementServiceBuilder defaultEnterpriseBuilder( Path storeDir, boolean backupEnabled )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( storeDir ).setConfig( online_backup_enabled, backupEnabled );
    }

    private static void createNodes( GraphDatabaseService db, int count )
    {
        try ( var tx = db.beginTx() )
        {
            for ( var i = 0; i < count; i++ )
            {
                tx.createNode( label( "Person" ) ).setProperty( "id", i );
            }
            tx.commit();
        }
    }

    private void verifyNodes( int count )
    {
        try ( var tx = db.beginTx() )
        {
            var ids = tx.findNodes( label( "Person" ) )
                    .stream()
                    .map( node -> node.getProperty( "id" ) )
                    .map( value -> (int) value )
                    .sorted()
                    .collect( Collectors.toList() );

            assertEquals( count, ids.size() );

            for ( var i = 0; i < ids.size(); i++ )
            {
                assertEquals( i, ids.get( i ).intValue() );
            }

            tx.commit();
        }
    }
}
