/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.com.storecopy.FileMoveAction;
import com.neo4j.com.storecopy.FileMoveProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

@PageCacheExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class BackupCopyServiceTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;

    private FileMoveProvider fileMoveProvider;
    private BackupCopyService backupCopyService;

    @BeforeEach
    void beforeEach()
    {
        fileMoveProvider = mock( FileMoveProvider.class );
        backupCopyService = new BackupCopyService( fs, fileMoveProvider, new StoreFiles( fs, pageCache ), NullLogProvider.getInstance(), NULL );
    }

    @Test
    void logicForMovingBackupsIsDelegatedToFileMoveProvider() throws IOException
    {
        // given
        Path parentDirectory = testDirectory.directory( "parent" );
        Path oldLocation = parentDirectory.resolve( "oldLocation" );
        Files.createDirectories( oldLocation );
        Path newLocation = parentDirectory.resolve( "newLocation" );

        // and
        FileMoveAction fileOneMoveAction = mock( FileMoveAction.class );
        FileMoveAction fileTwoMoveAction = mock( FileMoveAction.class );
        when( fileMoveProvider.traverseForMoving( any() ) ).thenReturn( Stream.of( fileOneMoveAction, fileTwoMoveAction ) );

        // when
        backupCopyService.moveBackupLocation( oldLocation, newLocation );

        // then file move propagator was requested with correct source and baseDirectory
        verify( fileMoveProvider ).traverseForMoving( oldLocation );

        // and files were moved to correct target directory
        verify( fileOneMoveAction ).move( newLocation );
        verify( fileTwoMoveAction ).move( newLocation );
    }

    @Test
    void shouldDeletePreExistingBrokenBackupWhenItHasSameStoreIdAsNewSuccessfulBackup() throws Exception
    {
        Path oldDir = testDirectory.homePath( "old" );
        Path newDir = testDirectory.homePath( "new" );

        startAndStopDb( oldDir );

        DatabaseLayout oldLayout = Neo4jLayout.of( oldDir ).databaseLayout( DEFAULT_DATABASE_NAME );
        DatabaseLayout newLayout = Neo4jLayout.of( newDir ).databaseLayout( DEFAULT_DATABASE_NAME );

        fs.copyRecursively( oldLayout.databaseDirectory(), newLayout.databaseDirectory() );

        assertTrue( fs.isDirectory( oldLayout.databaseDirectory() ) );
        assertTrue( fs.isDirectory( newLayout.databaseDirectory() ) );

        assertTrue( backupCopyService.deletePreExistingBrokenBackupIfPossible( oldLayout.databaseDirectory(), newLayout.databaseDirectory() ) );

        assertFalse( fs.fileExists( oldLayout.databaseDirectory() ) );
        assertTrue( fs.isDirectory( newLayout.databaseDirectory() ) );
    }

    @Test
    void shouldNotDeletePreExistingBrokenBackupWhenItHasDifferentStoreIdFromNewSuccessfulBackup() throws Exception
    {
        Path oldDir = testDirectory.directory( "old" );
        Path newDir = testDirectory.directory( "new" );

        startAndStopDb( oldDir );
        startAndStopDb( newDir );

        assertTrue( Files.isDirectory( oldDir ) );
        assertTrue( Files.isDirectory( newDir ) );

        assertFalse( backupCopyService.deletePreExistingBrokenBackupIfPossible( oldDir, newDir ) );

        assertTrue( Files.isDirectory( oldDir ) );
        assertTrue( Files.isDirectory( newDir ) );
    }

    @Test
    void shouldNotDeletePreExistingBrokenBackupWhenItsStoreIdIsUnreadable() throws Exception
    {
        Path oldDir = testDirectory.homePath( "old" );
        Path newDir = testDirectory.homePath( "new" );

        startAndStopDb( oldDir );
        startAndStopDb( newDir );

        DatabaseLayout oldLayout = Neo4jLayout.of( oldDir ).databaseLayout( DEFAULT_DATABASE_NAME );
        DatabaseLayout newLayout = Neo4jLayout.of( newDir ).databaseLayout( DEFAULT_DATABASE_NAME );

        assertTrue( fs.isDirectory( oldLayout.databaseDirectory() ) );
        assertTrue( fs.isDirectory( newLayout.databaseDirectory() ) );

        fs.deleteFileOrThrow( oldLayout.metadataStore() );

        assertFalse( backupCopyService.deletePreExistingBrokenBackupIfPossible( oldLayout.databaseDirectory(), newLayout.databaseDirectory() ) );

        assertTrue( fs.isDirectory( oldLayout.databaseDirectory() ) );
        assertTrue( fs.isDirectory( newLayout.databaseDirectory() ) );
    }

    @Test
    void shouldThrowWhenUnableToReadStoreIdFromNewSuccessfulBackup() throws Exception
    {
        Path oldDir = testDirectory.homePath( "old" );
        Path newDir = testDirectory.homePath( "new" );

        startAndStopDb( oldDir );
        startAndStopDb( newDir );

        DatabaseLayout oldLayout = Neo4jLayout.of( oldDir ).databaseLayout( DEFAULT_DATABASE_NAME );
        DatabaseLayout newLayout = Neo4jLayout.of( newDir ).databaseLayout( DEFAULT_DATABASE_NAME );

        assertTrue( fs.isDirectory( oldLayout.databaseDirectory() ) );
        assertTrue( fs.isDirectory( newLayout.databaseDirectory() ) );

        fs.deleteFileOrThrow( newLayout.metadataStore() );

        IOException error = assertThrows( IOException.class,
                                          () -> backupCopyService.deletePreExistingBrokenBackupIfPossible( oldLayout.databaseDirectory(),
                                                                                                           newLayout.databaseDirectory() ) );

        assertThat( error.getMessage(), containsString( "Unable to read store ID from the new successful backup" ) );
    }

    private static void startAndStopDb( Path databaseDir )
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseDir ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label( "Cat" ) ).setProperty( "name", "Tom" );
            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
