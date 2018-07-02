/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class BackupStrategyWrapperTest
{
    @Inject
    private TestDirectory testDirectory;

    private final BackupStrategy backupStrategyImplementation = mock( BackupStrategy.class );
    private final BackupCopyService backupCopyService = mock( BackupCopyService.class );

    private BackupStrategyWrapper backupWrapper;

    private OnlineBackupContext onlineBackupContext;

    private final FileSystemAbstraction fileSystemAbstraction = mock( FileSystemAbstraction.class );
    private DatabaseLayout desiredBackupLayout;
    private Path reportDir;
    private Path availableFreshBackupLocation;
    private Path availableOldBackupLocation;
    private final Config config = Config.defaults();
    private final AdvertisedSocketAddress address = new AdvertisedSocketAddress( "neo4j.com", 6362 );
    private final PageCache pageCache = mock( PageCache.class );
    private final LogProvider logProvider = mock( LogProvider.class );
    private final Log log = mock( Log.class );

    @BeforeEach
    void setup() throws Exception
    {
        desiredBackupLayout = testDirectory.databaseLayout( "desiredBackupLayout" );
        reportDir = testDirectory.directory( "reportDir" ).toPath();
        availableFreshBackupLocation = testDirectory.directory( "availableFreshBackupLocation" ).toPath();
        availableOldBackupLocation = testDirectory.directory( "availableOldBackupLocation" ).toPath();

        when( pageCache.map( any(), anyInt(), any() ) ).thenReturn( mock( PagedFile.class, RETURNS_MOCKS ) );
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( any() ) ).thenReturn( availableFreshBackupLocation );
        when( backupCopyService.findNewBackupLocationForBrokenExisting( any() ) ).thenReturn( availableOldBackupLocation );
        when( logProvider.getLog( (Class<?>) any() ) ).thenReturn( log );

        backupWrapper = spy( new TestBackupStrategyWrapper() );
    }

    @Test
    void lifecycleIsRun() throws Throwable
    {
        // given
        onlineBackupContext = newBackupContext( true );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).init();
        verify( backupStrategyImplementation ).start();
        verify( backupStrategyImplementation ).stop();
        verify( backupStrategyImplementation ).shutdown();
    }

    @Test
    void fullBackupIsPerformedWhenNoOtherBackupExists() throws Exception
    {
        // given
        onlineBackupContext = newBackupContext( true );

        // and
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).performFullBackup( any(), any() );
    }

    @Test
    void fullBackupIsIgnoredIfIncrementalFailAndNotFallback() throws Exception
    {
        // given there is an existing backup
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and we don't want to fallback to full backups
        onlineBackupContext = newBackupContext( false );

        // and incremental backup fails because it's a different store
        BackupExecutionException incrementalBackupError = new BackupExecutionException( "Store mismatch" );
        doThrow( incrementalBackupError ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> backupWrapper.doBackup( onlineBackupContext ) );

        // then full backup wasnt performed
        verify( backupStrategyImplementation, never() ).performFullBackup( any(), any() );
        assertEquals( incrementalBackupError, error );
    }

    @Test
    void fullBackupIsPerformedIfNotIncrementalEvenThoughFallBackToFullIsFalse() throws BackupExecutionException
    {
        // given there is an existing backup
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and we don't want to fallback to full backups
        onlineBackupContext = newBackupContext( false );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then full backup was performed
        verify( backupStrategyImplementation, never() ).performIncrementalBackup( any(), any() );
        verify( backupStrategyImplementation ).performFullBackup( any(), any() );
    }

    @Test
    void fullBackupIsNotPerformedWhenAnIncrementalBackupIsSuccessful() throws Exception
    {
        // given
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and
        onlineBackupContext = newBackupContext( true );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation, never() ).performFullBackup( desiredBackupLayout, address );
    }

    @Test
    void failedIncrementalFallsBackToFullWhenOptionSet() throws Exception
    {
        // given conditions for incremental exist
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        onlineBackupContext = newBackupContext( true );

        // and incremental fails
        doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );

        // when
        assertDoesNotThrow( () -> backupWrapper.doBackup( onlineBackupContext ) );

        // then
        InOrder inOrder = inOrder( backupStrategyImplementation );
        inOrder.verify( backupStrategyImplementation ).performIncrementalBackup( any(), any() );
        inOrder.verify( backupStrategyImplementation ).performFullBackup( any(), any() );
    }

    @Test
    void fallbackDoesNotHappenIfNotSpecified() throws Exception
    {
        // given
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        BackupExecutionException incrementalBackupError = new BackupExecutionException( "Oops" );
        doThrow( incrementalBackupError ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );

        // and
        onlineBackupContext = newBackupContext( false );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> backupWrapper.doBackup( onlineBackupContext ) );

        // then
        verify( backupStrategyImplementation, never() ).performFullBackup( any(), any() );
        assertEquals( incrementalBackupError, error );
    }

    @Test
    void failedBackupsDontMoveExisting() throws Exception
    {
        // given a backup already exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and fallback to full is true
        onlineBackupContext = newBackupContext( true );

        // and an incremental backup fails
        doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );

        // and full backup fails
        doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performFullBackup( any(), any() );

        // when backup is performed
        assertThrows( BackupExecutionException.class, () -> backupWrapper.doBackup( onlineBackupContext ) );

        // then existing backup hasn't moved
        verify( backupStrategyImplementation ).performFullBackup( any(), any() );
        verify( backupCopyService, never() ).moveBackupLocation( any(), any() );
    }

    @Test
    void successfulFullBackupsMoveExistingBackup() throws Exception
    {
        // given backup exists
        File backupsDir = testDirectory.directory( "backups" );
        File databaseBackupDir = new File( backupsDir, DEFAULT_DATABASE_NAME );

        desiredBackupLayout = DatabaseLayout.of( backupsDir );
        when( backupCopyService.backupExists( DatabaseLayout.of( databaseBackupDir ) ) ).thenReturn( true );

        // and fallback to full flag has been set
        onlineBackupContext = newBackupContext( true );

        // and a new location for the existing backup is found
        Path newLocationForExistingBackup = testDirectory.directory( "new-backup-location" ).toPath();
        when( backupCopyService.findNewBackupLocationForBrokenExisting( databaseBackupDir.toPath() ) )
                .thenReturn( newLocationForExistingBackup );

        // and there is a generated location for where to store a new full backup so the original is not destroyed
        Path temporaryFullBackupLocation = testDirectory.directory( "temporary-full-backup" ).toPath();
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( databaseBackupDir.toPath() ) )
                .thenReturn( temporaryFullBackupLocation );

        // and incremental fails
        doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );

        // and full passes
        doNothing().when( backupStrategyImplementation ).performFullBackup( any(), any() );

        // when
        assertDoesNotThrow( () -> backupWrapper.doBackup( onlineBackupContext ) );

        // then original existing backup is moved to err directory
        verify( backupCopyService ).moveBackupLocation( databaseBackupDir.toPath(), newLocationForExistingBackup );

        // and new successful backup is renamed to original expected name
        verify( backupCopyService ).moveBackupLocation( temporaryFullBackupLocation, databaseBackupDir.toPath() );
    }

    @Test
    void failureDuringMoveCausesException() throws Exception
    {
        // given moves fail
        IOException ioException = new IOException();
        doThrow( ioException ).when( backupCopyService ).moveBackupLocation( any(), any() );

        // and fallback to full
        onlineBackupContext = newBackupContext( true );

        // and backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and incremental fails
        doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );

        // and full passes
        doNothing().when( backupStrategyImplementation ).performFullBackup( any(), any() );

        // when
        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> backupWrapper.doBackup( onlineBackupContext ) );

        // then result was catastrophic and contained reason
        assertEquals( ioException, error.getCause() );

        // and full backup was definitely executed
        verify( backupStrategyImplementation ).performFullBackup( any(), any() );
    }

    @Test
    void performingFullBackupInvokesRecovery() throws Exception
    {
        // given full backup flag is set
        onlineBackupContext = newBackupContext( true );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper ).performRecovery( eq( config ), any( DatabaseLayout.class ) );
    }

    @Test
    void performingIncrementalBackupInvokesRecovery() throws Exception
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        onlineBackupContext = newBackupContext( true );

        // and incremental backups are successful
        doNothing().when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper ).performRecovery( any(), any() );
    }

    @Test
    void successfulBackupsAreRecovered() throws Exception
    {
        // given
        fallbackToFullPasses();
        onlineBackupContext = newBackupContext( true );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper ).performRecovery( eq( config ), any( DatabaseLayout.class ) );
    }

    @Test
    void unsuccessfulBackupsAreNotRecovered() throws Exception
    {
        // given
        bothBackupsFail();
        onlineBackupContext = newBackupContext( true );

        // when
        assertThrows( BackupExecutionException.class, () -> backupWrapper.doBackup( onlineBackupContext ) );

        // then
        verify( backupWrapper, never() ).performRecovery( any(), any() );
    }

    @Test
    void successfulFullBackupsAreRecoveredEvenIfNoBackupExisted() throws Exception
    {
        // given a backup exists
        when( backupCopyService.backupExists( desiredBackupLayout ) ).thenReturn( false );
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( desiredBackupLayout.databaseDirectory().toPath() ) )
                .thenReturn( desiredBackupLayout.databaseDirectory().toPath() );

        // and
        fallbackToFullPasses();
        onlineBackupContext = newBackupContext( true );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper ).performRecovery( eq( config ), any( DatabaseLayout.class ) );
    }

    @Test
    void recoveryIsPerformedBeforeRename() throws Exception
    {
        // given
        fallbackToFullPasses();
        onlineBackupContext = newBackupContext( true );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        Path databaseBackupDir = desiredBackupLayout.file( DEFAULT_DATABASE_NAME ).toPath();

        InOrder inOrder = Mockito.inOrder( backupWrapper, backupCopyService );
        // 1) perform recovery
        inOrder.verify( backupWrapper ).performRecovery( eq( config ), any( DatabaseLayout.class ) );
        // 2) move pre-existing backup to a different directory
        inOrder.verify( backupCopyService ).moveBackupLocation( databaseBackupDir, availableOldBackupLocation );
        // 3) move new backup from a temporary directory to the specified directory
        inOrder.verify( backupCopyService ).moveBackupLocation( availableFreshBackupLocation, databaseBackupDir );
    }

    @Test
    void logsWhenIncrementalFailsAndFallbackToFull() throws Exception
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and fallback to full
        fallbackToFullPasses();
        onlineBackupContext = newBackupContext( true );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( log ).info( "Previous backup not found, a new full backup will be performed." );
    }

    private void incrementalBackupIsSuccessful( boolean isSuccessful ) throws Exception
    {
        if ( isSuccessful )
        {
            doNothing().when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );
        }
        else
        {
            doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );
        }
    }

    private void bothBackupsFail() throws Exception
    {
        doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );
        doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performFullBackup( any(), any() );
    }

    private void fallbackToFullPasses() throws Exception
    {
        doThrow( BackupExecutionException.class ).when( backupStrategyImplementation ).performIncrementalBackup( any(), any() );
        doNothing().when( backupStrategyImplementation ).performFullBackup( any(), any() );
    }

    private OnlineBackupContext newBackupContext( boolean fallbackToFull )
    {
        return OnlineBackupContext.builder()
                .withAddress( address )
                .withConfig( config )
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withBackupDirectory( desiredBackupLayout.databaseDirectory().toPath() )
                .withFallbackToFullBackup( fallbackToFull )
                .withConsistencyCheck( true )
                .withReportsDirectory( reportDir )
                .build();
    }

    private class TestBackupStrategyWrapper extends BackupStrategyWrapper
    {
        TestBackupStrategyWrapper()
        {
            super( BackupStrategyWrapperTest.this.backupStrategyImplementation, BackupStrategyWrapperTest.this.backupCopyService,
                    BackupStrategyWrapperTest.this.fileSystemAbstraction, BackupStrategyWrapperTest.this.pageCache, NullLogProvider.getInstance(),
                    BackupStrategyWrapperTest.this.logProvider );
        }

        @Override
        void performRecovery( Config config, DatabaseLayout backupLayout )
        {
            // empty recovery for mock tests
        }
    }
}
