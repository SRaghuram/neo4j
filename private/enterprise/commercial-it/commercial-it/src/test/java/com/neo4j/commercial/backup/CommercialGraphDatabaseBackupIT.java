/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.backup;

import com.neo4j.backup.BackupTestUtil;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.helpers.CausalClusteringTestHelpers.backupAddress;
import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.graphdb.Label.label;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class CommercialGraphDatabaseBackupIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    void shouldDoBackup() throws Exception
    {
        int nodeCount = 999;
        db = newCommercialDb( testDirectory.storeDir(), true );
        createNodes( db, nodeCount );

        File backupDir = performBackup( testDirectory.databaseDir() );
        db.shutdown();

        db = newCommercialBackupDb( backupDir, false );
        verifyNodes( nodeCount );

        db.shutdown();
    }

    private File performBackup( File storeDir ) throws Exception
    {
        File backupsDir = testDirectory.directory( "backups" );

        int exitCode = BackupTestUtil.runBackupToolFromOtherJvmToGetExitCode( storeDir,
                "--from=" + backupAddress( db ),
                "--backup-dir=" + backupsDir,
                "--database=" + DEFAULT_DATABASE_NAME );

        assertEquals( 0, exitCode );

        return new File( backupsDir, DEFAULT_DATABASE_NAME );
    }

    private static GraphDatabaseAPI newCommercialDb( File storeDir, boolean backupEnabled )
    {
        DatabaseManagementService managementService = defaultCommercialBuilder( storeDir, backupEnabled ).newDatabaseManagementService();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static GraphDatabaseAPI newCommercialBackupDb( File databaseDirectory, boolean backupEnabled )
    {
        File storeDir = databaseDirectory.getParentFile();
        DatabaseManagementService managementService = defaultCommercialBuilder( storeDir, backupEnabled )
                .setConfig( transaction_logs_root_path, storeDir.getAbsolutePath() ).newDatabaseManagementService();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static GraphDatabaseBuilder defaultCommercialBuilder( File storeDir, boolean backupEnabled )
    {
        return new TestCommercialGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( online_backup_enabled, Boolean.toString( backupEnabled ) );
    }

    private static void createNodes( GraphDatabaseService db, int count )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < count; i++ )
            {
                db.createNode( label( "Person" ) ).setProperty( "id", i );
            }
            tx.success();
        }
    }

    private void verifyNodes( int count )
    {
        try ( Transaction tx = db.beginTx() )
        {
            List<Integer> ids = db.findNodes( label( "Person" ) )
                    .stream()
                    .map( node -> node.getProperty( "id" ) )
                    .map( value -> (int) value )
                    .sorted()
                    .collect( Collectors.toList() );

            assertEquals( count, ids.size() );

            for ( int i = 0; i < ids.size(); i++ )
            {
                assertEquals( i, ids.get( i ).intValue() );
            }

            tx.success();
        }
    }
}
