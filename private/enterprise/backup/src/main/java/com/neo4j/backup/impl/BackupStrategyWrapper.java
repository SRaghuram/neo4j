/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.v4.metadata.IncludeMetadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * Individual backup strategies can perform incremental backups and full backups. The logic of how and when to perform full/incremental is identical. This class
 * describes the behaviour of a single strategy and is used to wrap an interface providing incremental/full backup functionality
 */
class BackupStrategyWrapper
{
    private final BackupStrategy backupStrategy;
    private final BackupCopyService backupCopyService;
    private final Log userLog;
    private final Log debugLog;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;

    BackupStrategyWrapper( BackupStrategy backupStrategy, BackupCopyService backupCopyService, FileSystemAbstraction fs, PageCache pageCache,
                           LogProvider userLogProvider, LogProvider logProvider )
    {
        this.backupStrategy = backupStrategy;
        this.backupCopyService = backupCopyService;
        this.fs = fs;
        this.pageCache = pageCache;
        this.userLog = userLogProvider.getLog( getClass() );
        this.debugLog = logProvider.getLog( getClass() );
    }

    /**
     * Try to do a backup using the given strategy (ex. BackupProtocol). This covers all stages (starting with incremental and falling back to a a full backup).
     * The end result of this method will either be a successful backup or any other return type with the reason why the backup wasn't successful
     *
     * @param onlineBackupContext the command line arguments, configuration, flags
     */
    void doBackup( OnlineBackupContext onlineBackupContext ) throws BackupExecutionException
    {
        try ( Lifespan ignore = new Lifespan( backupStrategy ) )
        {
            performBackupWithoutLifecycle( onlineBackupContext );
        }
    }

    private void performBackupWithoutLifecycle( OnlineBackupContext onlineBackupContext ) throws BackupExecutionException
    {
        var backupLocation = onlineBackupContext.getDatabaseBackupDir();
        var address = onlineBackupContext.getAddress();
        var config = onlineBackupContext.getConfig();
        var memoryTracker = onlineBackupContext.getMemoryTracker();
        var backupLayout = DatabaseLayout.ofFlat( backupLocation );
        var includeMetadata = onlineBackupContext.getIncludeMetadata();

        var previousBackupExists = backupCopyService.backupExists( backupLayout );
        var fallbackToFull = onlineBackupContext.fallbackToFullBackupEnabled();

        if ( previousBackupExists )
        {
            debugLog.info( "Previous backup found, trying incremental backup." );
            if ( tryIncrementalBackup( backupLayout, config, address, fallbackToFull, onlineBackupContext.getDatabaseName(), includeMetadata, memoryTracker ) )
            {
                return;
            }
        }

        if ( previousBackupExists && fallbackToFull )
        {
            debugLog.info( "Incremental backup failed, a new full backup will be performed." );
            fullBackupWithTemporaryFolderResolutions( onlineBackupContext, onlineBackupContext.getDatabaseName() );
        }
        else if ( !previousBackupExists )
        {
            debugLog.info( "Previous backup not found, a new full backup will be performed." );
            fullBackupWithTemporaryFolderResolutions( onlineBackupContext, onlineBackupContext.getDatabaseName() );
        }
        else
        {
            throw new BackupExecutionException( "Incremental backup failed but fallback to full backup is disallowed by configuration" );
        }
    }

    private boolean tryIncrementalBackup( DatabaseLayout backupLayout, Config config, SocketAddress address, boolean fallbackToFullAllowed,
                                          String databaseName, Optional<IncludeMetadata> includeMetadata, MemoryTracker memoryTracker )
            throws BackupExecutionException
    {
        try
        {
            backupStrategy.performIncrementalBackup( backupLayout, address, databaseName, includeMetadata );
            performRecovery( config, backupLayout, memoryTracker );
            return true;
        }
        catch ( Exception e )
        {
            if ( fallbackToFullAllowed )
            {
                debugLog.warn( "Incremental backup failed", e );
                return false;
            }
            else
            {
                throw e;
            }
        }
    }

    /**
     * This will perform a full backup with some directory renaming if necessary.
     * <p>
     * If there is no existing backup, then no renaming will occur. Otherwise the full backup will be done into a temporary directory and renaming will occur if
     * everything was successful.
     * </p>
     *
     * @param onlineBackupContext command line arguments, config etc.
     * @param databaseName
     */
    private void fullBackupWithTemporaryFolderResolutions( OnlineBackupContext onlineBackupContext, String databaseName ) throws BackupExecutionException
    {
        var userSpecifiedBackupLocation = onlineBackupContext.getDatabaseBackupDir();
        var memoryTracker = onlineBackupContext.getMemoryTracker();
        var temporaryFullBackupLocation = backupCopyService.findAnAvailableLocationForNewFullBackup( userSpecifiedBackupLocation );
        var backupToATemporaryLocation = !userSpecifiedBackupLocation.equals( temporaryFullBackupLocation );

        if ( backupToATemporaryLocation )
        {
            userLog.info(
                    "Full backup will be first performed to a temporary directory '%s' because the specified directory '%s' already exists and is not empty",
                    temporaryFullBackupLocation, userSpecifiedBackupLocation );
        }

        var address = onlineBackupContext.getAddress();
        var backupLayout = DatabaseLayout.ofFlat( temporaryFullBackupLocation );
        var includeMetadata = onlineBackupContext.getIncludeMetadata();
        backupStrategy.performFullBackup( backupLayout, address, databaseName, includeMetadata );

        performRecovery( onlineBackupContext.getConfig(), backupLayout, memoryTracker );

        if ( backupToATemporaryLocation )
        {
            renameTemporaryBackupToExpected( temporaryFullBackupLocation, userSpecifiedBackupLocation );
        }
    }

    @VisibleForTesting
    void performRecovery( Config config, DatabaseLayout backupLayout, MemoryTracker memoryTracker ) throws BackupExecutionException
    {
        try
        {
            Recovery.performRecovery( fs, pageCache, DatabaseTracers.EMPTY, config, backupLayout, memoryTracker );
        }
        catch ( IOException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    private void renameTemporaryBackupToExpected( Path temporaryFullBackupLocation, Path userSpecifiedBackupLocation ) throws BackupExecutionException
    {
        try
        {
            var deletedPreExistingBackup =
                    backupCopyService.deletePreExistingBrokenBackupIfPossible( userSpecifiedBackupLocation, temporaryFullBackupLocation );

            if ( !deletedPreExistingBackup )
            {
                Path newBackupLocationForPreExistingBackup = backupCopyService.findNewBackupLocationForBrokenExisting( userSpecifiedBackupLocation );

                userLog.info( "Moving pre-existing directory '%s' that does not contain a valid backup to '%s'",
                              userSpecifiedBackupLocation, newBackupLocationForPreExistingBackup );
                backupCopyService.moveBackupLocation( userSpecifiedBackupLocation, newBackupLocationForPreExistingBackup );
            }

            userLog.info( "Moving temporary backup directory '%s' to the specified directory '%s'",
                          temporaryFullBackupLocation, userSpecifiedBackupLocation );
            backupCopyService.moveBackupLocation( temporaryFullBackupLocation, userSpecifiedBackupLocation );
        }
        catch ( IOException e )
        {
            throw new BackupExecutionException( e );
        }
    }
}
