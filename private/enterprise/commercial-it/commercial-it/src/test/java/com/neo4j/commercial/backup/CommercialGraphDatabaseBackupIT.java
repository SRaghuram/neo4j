/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.backup;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.runBackupToolFromOtherJvmToGetExitCode;
import static com.neo4j.backup.BackupTestUtil.runBackupToolFromSameJvm;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.backupAddress;
import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.graphdb.Label.label;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
@ResourceLock( Resources.SYSTEM_OUT )
class CommercialGraphDatabaseBackupIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private SuppressOutput suppressOutput;

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
        db = newCommercialDb( testDirectory.storeDir(), true );
        createNodes( db, nodeCount );

        var backupDir = performBackup();
        managementService.shutdown();

        db = newCommercialBackupDb( backupDir, false );
        verifyNodes( nodeCount );

        managementService.shutdown();
    }

    @Test
    void shouldFailWithErrorMessageForUnknownDatabase() throws Exception
    {
        var unknownDbName = "unknown_db";
        db = newCommercialDb( testDirectory.storeDir(), true );

        var exitCode = runBackupToolFromSameJvm( testDirectory.databaseDir(),
                "--from=" + backupAddress( db ),
                "--backup-dir=" + testDirectory.directory( "backups" ),
                "--database=" + unknownDbName );

        assertEquals( 1, exitCode );

        assertThat( suppressOutput.getErrorVoice().lines(), hasItem( containsString( "Database '" + unknownDbName + "' does not exist" ) ) );
    }

    private File performBackup() throws Exception
    {
        var storeDir = testDirectory.databaseDir();
        var backupsDir = testDirectory.directory( "backups" );

        var exitCode = runBackupToolFromOtherJvmToGetExitCode( storeDir,
                "--from=" + backupAddress( db ),
                "--backup-dir=" + backupsDir,
                "--database=" + DEFAULT_DATABASE_NAME );

        assertEquals( 0, exitCode );

        return new File( backupsDir, DEFAULT_DATABASE_NAME );
    }

    private GraphDatabaseAPI newCommercialDb( File storeDir, boolean backupEnabled )
    {
        managementService = defaultCommercialBuilder( storeDir, backupEnabled ).build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private GraphDatabaseAPI newCommercialBackupDb( File databaseDirectory, boolean backupEnabled )
    {
        var storeDir = databaseDirectory.getParentFile();
        managementService = defaultCommercialBuilder( storeDir, backupEnabled )
                .setConfig( transaction_logs_root_path, storeDir.toPath().toAbsolutePath() ).build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static DatabaseManagementServiceBuilder defaultCommercialBuilder( File storeDir, boolean backupEnabled )
    {
        return new TestCommercialDatabaseManagementServiceBuilder( storeDir ).setConfig( online_backup_enabled, backupEnabled );
    }

    private static void createNodes( GraphDatabaseService db, int count )
    {
        try ( var tx = db.beginTx() )
        {
            for ( var i = 0; i < count; i++ )
            {
                db.createNode( label( "Person" ) ).setProperty( "id", i );
            }
            tx.commit();
        }
    }

    private void verifyNodes( int count )
    {
        try ( var tx = db.beginTx() )
        {
            var ids = db.findNodes( label( "Person" ) )
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
