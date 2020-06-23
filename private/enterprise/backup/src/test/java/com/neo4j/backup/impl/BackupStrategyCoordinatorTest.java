/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class BackupStrategyCoordinatorTest
{
    @Inject
    private TestDirectory testDirectory;

    // dependencies
    private final ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );
    private final FileSystemAbstraction fileSystem = mock( FileSystemAbstraction.class );
    private final LogProvider logProvider = mock( LogProvider.class );
    private final BackupStrategyWrapper firstStrategy = mock( BackupStrategyWrapper.class );

    private BackupStrategyCoordinator subject;

    // test method parameter mocks
    private final OnlineBackupContext onlineBackupContext = mock( OnlineBackupContext.class );

    // mock returns
    private final ProgressMonitorFactory progressMonitorFactory = mock( ProgressMonitorFactory.class );
    private final ConsistencyCheckService.Result consistencyCheckResult = mock( ConsistencyCheckService.Result.class );

    @BeforeEach
    void setup()
    {
        Path reportsDir = testDirectory.directoryPath( "reports" );
        Path backupsDir = testDirectory.directoryPath( "backups" );

        when( fileSystem.isDirectory( any() ) ).thenReturn( true );
        when( onlineBackupContext.getReportDir() ).thenReturn( reportsDir );
        when( onlineBackupContext.getDatabaseBackupDir() ).thenReturn( backupsDir.resolve( DEFAULT_DATABASE_NAME ) );
        subject = new BackupStrategyCoordinator( fileSystem, consistencyCheckService, logProvider, progressMonitorFactory, firstStrategy );
    }

    @Test
    void consistencyCheckIsRunIfSpecified() throws Exception
    {
        // given
        when( onlineBackupContext.consistencyCheckEnabled() ).thenReturn( true );
        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), eq( progressMonitorFactory ), any( LogProvider.class ), any(), eq( false ), any(),
                any() ) ).thenReturn( consistencyCheckResult );
        when( consistencyCheckResult.isSuccessful() ).thenReturn( true );

        // when
        subject.performBackup( onlineBackupContext );

        // then
        verify( consistencyCheckService ).runFullConsistencyCheck( any(), any(), any(), any(), any(), eq( false ), any(), any() );
    }

    @Test
    void consistencyCheckIsNotRunIfNotSpecified() throws Exception
    {
        // given
        when( onlineBackupContext.consistencyCheckEnabled() ).thenReturn( false );

        // when
        subject.performBackup( onlineBackupContext );

        // then
        verify( consistencyCheckService, never() ).runFullConsistencyCheck( any(), any(), any(), any(), any(), eq( false ), any(),
                any( ConsistencyFlags.class ) );
    }

    @Test
    void commandFailedWhenConsistencyCheckFails() throws Exception
    {
        // given
        when( onlineBackupContext.consistencyCheckEnabled() ).thenReturn( true );
        when( consistencyCheckResult.isSuccessful() ).thenReturn( false );
        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), eq( progressMonitorFactory ), any( LogProvider.class ), any(), eq( false ), any(),
                any() ) ).thenReturn( consistencyCheckResult );

        // when
        ConsistencyCheckExecutionException error = assertThrows( ConsistencyCheckExecutionException.class, () -> subject.performBackup( onlineBackupContext ) );

        // when
        assertThat( error.getMessage(), containsString( "Inconsistencies found" ) );
    }
}
