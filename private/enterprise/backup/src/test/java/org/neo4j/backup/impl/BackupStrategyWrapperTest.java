/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyVararg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( TestDirectoryExtension.class )
class BackupStrategyWrapperTest
{
    @Inject
    private TestDirectory testDirectory;

    private final BackupStrategy backupStrategyImplementation = mock( BackupStrategy.class );
    private final OutsideWorld outsideWorld = mock( OutsideWorld.class );
    private final BackupCopyService backupCopyService = mock( BackupCopyService.class );

    private BackupStrategyWrapper backupWrapper;

    private OnlineBackupContext onlineBackupContext;

    private final FileSystemAbstraction fileSystemAbstraction = mock( FileSystemAbstraction.class );
    private DatabaseLayout desiredBackupLayout;
    private Path reportDir;
    private Path availableFreshBackupLocation;
    private OnlineBackupRequiredArguments requiredArguments;
    private final Config config = mock( Config.class );
    private final OptionalHostnamePort userProvidedAddress = new OptionalHostnamePort( (String) null, null, null );
    private final Fallible<BackupStageOutcome> SUCCESS = new Fallible<>( BackupStageOutcome.SUCCESS, null );
    private final Fallible<BackupStageOutcome> FAILURE = new Fallible<>( BackupStageOutcome.FAILURE, null );
    private final PageCache pageCache = mock( PageCache.class );
    private final LogProvider logProvider = mock( LogProvider.class );
    private final Log log = mock( Log.class );

    @BeforeEach
    void setup() throws IOException
    {
        desiredBackupLayout = testDirectory.databaseLayout( "desiredBackupLayout" );
        reportDir = testDirectory.directory( "reportDir" ).toPath();
        availableFreshBackupLocation = testDirectory.directory( "availableFreshBackupLocation" ).toPath();
        Path availableOldBackupLocation = testDirectory.directory( "availableOldBackupLocation" ).toPath();

        when( pageCache.map( any(), anyInt(), anyVararg() ) ).thenReturn( mock( PagedFile.class, RETURNS_MOCKS ) );
        when( outsideWorld.fileSystem() ).thenReturn( fileSystemAbstraction );
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( any() ) ).thenReturn( availableFreshBackupLocation );
        when( backupCopyService.findNewBackupLocationForBrokenExisting( any() ) ).thenReturn( availableOldBackupLocation );
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn( SUCCESS );
        when( logProvider.getLog( (Class) any() ) ).thenReturn( log );

        backupWrapper =
                spy( new BackupStrategyWrapper( backupStrategyImplementation, backupCopyService, fileSystemAbstraction, pageCache, config, logProvider ) );
    }

    @Test
    void lifecycleIsRun() throws Throwable
    {
        // given
        onlineBackupContext = new OnlineBackupContext( requiredArguments( true ), config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).init();
        verify( backupStrategyImplementation ).start();
        verify( backupStrategyImplementation ).stop();
        verify( backupStrategyImplementation ).shutdown();
    }

    @Test
    void fullBackupIsPerformedWhenNoOtherBackupExists()
    {
        // given
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
    }

    @Test
    void fullBackupIsIgnoredIfNoOtherBackupAndNotFallback()
    {
        // given there is an existing backup
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and we don't want to fallback to full backups
        requiredArguments = requiredArguments( false );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and incremental backup fails because it's a different store
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( FAILURE );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then full backup wasnt performed
        verify( backupStrategyImplementation, never() ).performFullBackup( any(), any(), any() );
    }

    @Test
    void fullBackupIsNotPerformedWhenAnIncrementalBackupIsSuccessful()
    {
        // given
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( SUCCESS );

        // and
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation, never() ).performFullBackup( desiredBackupLayout, config, userProvidedAddress );
    }

    @Test
    void failedIncrementalFallsBackToFullWhenOptionSet()
    {
        // given conditions for incremental exist
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
    }

    @Test
    void fallbackDoesNotHappenIfNotSpecified()
    {
        // given
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // and
        requiredArguments = requiredArguments( false );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation, never() ).performFullBackup( any(), any(), any() );
    }

    @Test
    void failedBackupsDontMoveExisting() throws IOException
    {
        // given a backup already exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and fallback to full is true
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and an incremental backup fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // and full backup fails
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // when backup is performed
        backupWrapper.doBackup( onlineBackupContext );

        // then existing backup hasn't moved
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
        verify( backupCopyService, never() ).moveBackupLocation( any(), any() );
    }

    @Test
    void successfulFullBackupsMoveExistingBackup() throws IOException
    {
        // given backup exists
        desiredBackupLayout = testDirectory.databaseLayout( "some-preexisting-backup" );
        when( backupCopyService.backupExists( desiredBackupLayout ) ).thenReturn( true );

        // and fallback to full flag has been set
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and a new location for the existing backup is found
        Path newLocationForExistingBackup = testDirectory.directory( "new-backup-location" ).toPath();
        when( backupCopyService.findNewBackupLocationForBrokenExisting( desiredBackupLayout.databaseDirectory().toPath() ) )
                .thenReturn( newLocationForExistingBackup );

        // and there is a generated location for where to store a new full backup so the original is not destroyed
        Path temporaryFullBackupLocation = testDirectory.directory( "temporary-full-backup" ).toPath();
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( desiredBackupLayout.databaseDirectory().toPath() ) )
                .thenReturn( temporaryFullBackupLocation );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // and full passes
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.SUCCESS, null ) );

        // when
        Fallible<BackupStrategyOutcome> state = backupWrapper.doBackup( onlineBackupContext );

        // then original existing backup is moved to err directory
        verify( backupCopyService ).moveBackupLocation( eq( desiredBackupLayout.databaseDirectory().toPath() ), eq( newLocationForExistingBackup ) );

        // and new successful backup is renamed to original expected name
        verify( backupCopyService ).moveBackupLocation( eq( temporaryFullBackupLocation ), eq( desiredBackupLayout.databaseDirectory().toPath() ) );

        // and backup was successful
        assertEquals( BackupStrategyOutcome.SUCCESS, state.getState() );
    }

    @Test
    void failureDuringMoveCausesAbsoluteFailure() throws IOException
    {
        // given moves fail
        doThrow( IOException.class ).when( backupCopyService ).moveBackupLocation( any(), any() );

        // and fallback to full
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // and full passes
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.SUCCESS, null ) );

        // when
        Fallible<BackupStrategyOutcome> state = backupWrapper.doBackup( onlineBackupContext );

        // then result was catastrophic and contained reason
        assertEquals( BackupStrategyOutcome.ABSOLUTE_FAILURE, state.getState() );
        assertEquals( IOException.class, state.getCause().get().getClass() );

        // and full backup was definitely performed
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
    }

    @Test
    void performingFullBackupInvokesRecovery() throws IOException
    {
        // given full backup flag is set
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper ).recoverBackup( any() );
    }

    @Test
    void performingIncrementalBackupDoesNotInvokeRecovery() throws IOException
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and incremental backups are successful
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( SUCCESS );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper, never() ).recoverBackup( any() );
    }

    @Test
    void successfulBackupsAreRecovered() throws IOException
    {
        // given
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper ).recoverBackup( any() );
    }

    @Test
    void unsuccessfulBackupsAreNotRecovered() throws IOException
    {
        // given
        bothBackupsFail();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper, never() ).recoverBackup( any() );
    }

    @Test
    void successfulFullBackupsAreRecoveredEvenIfNoBackupExisted() throws IOException
    {
        // given a backup exists
        when( backupCopyService.backupExists( desiredBackupLayout ) ).thenReturn( false );
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( desiredBackupLayout.databaseDirectory().toPath() ) )
                .thenReturn( desiredBackupLayout.databaseDirectory().toPath() );

        // and
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupWrapper ).recoverBackup( any() );
    }

    @Test
    void recoveryIsPerformedBeforeRename() throws IOException
    {
        // given
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupCopyService ).moveBackupLocation( eq( availableFreshBackupLocation ),
                eq( desiredBackupLayout.databaseDirectory().toPath() ) );
    }

    @Test
    void logsAreClearedAfterIncrementalBackup() throws IOException
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and
        incrementalBackupIsSuccessful( true );

        // and
        requiredArguments = requiredArguments(false);
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupCopyService ).clearIdFiles( any() );
    }

    @Test
    void logsAreNotClearedWhenIncrementalNotSuccessful() throws IOException
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and incremental is not successful
        incrementalBackupIsSuccessful( false );

        // and
        requiredArguments = requiredArguments(false);
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when backups are performed
        backupWrapper.doBackup( onlineBackupContext );

        // then do not
        verify( backupCopyService, never() ).clearIdFiles( any() );
    }

    @Test
    void logsAreClearedWhenFullBackupIsSuccessful() throws IOException
    {
        // given a backup doesn't exist
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupCopyService ).clearIdFiles( any() );
    }

    @Test
    void logsAreNotClearedWhenFullBackupIsNotSuccessful() throws IOException
    {
        // given a backup doesn't exist
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and
        bothBackupsFail();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( backupCopyService, never() ).clearIdFiles( any() );
    }

    @Test
    void logsWhenIncrementalFailsAndFallbackToFull()
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and fallback to full
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        backupWrapper.doBackup( onlineBackupContext );

        // then
        verify( log ).info( "Previous backup not found, a new full backup will be performed." );
    }

    // ====================================================================================================

    private void incrementalBackupIsSuccessful( boolean isSuccessful )
    {
        if ( isSuccessful )
        {
            when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                    new Fallible<>( BackupStageOutcome.SUCCESS, null ) );
            return;
        }
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );
    }

    private void bothBackupsFail()
    {
        requiredArguments = requiredArguments( true );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( FAILURE );
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn( FAILURE );
    }

    private void fallbackToFullPasses()
    {
        requiredArguments = requiredArguments( true );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( FAILURE );
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn( SUCCESS );
    }

    private OnlineBackupRequiredArguments requiredArguments( boolean fallbackToFull )
    {
        File databaseDirectory = desiredBackupLayout.databaseDirectory();
        return new OnlineBackupRequiredArguments( userProvidedAddress, desiredBackupLayout.getStoreLayout().storeDirectory().toPath(),
                databaseDirectory.getName(), SelectedBackupProtocol.ANY, fallbackToFull, true, 1000, reportDir );
    }

    private static ConsistencyFlags consistencyFlags()
    {
        return new ConsistencyFlags( true, true, true, true );
    }
}
