/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

/**
 * Individual backup strategies can perform incremental backups and full backups. The logic of how and when to perform full/incremental is identical.
 * This class describes the behaviour of a single strategy and is used to wrap an interface providing incremental/full backup functionality
 */
class BackupStrategyWrapper
{
    private final BackupStrategy backupStrategy;
    private final BackupCopyService backupCopyService;
    private final Log log;

    private final FileSystemAbstraction fs;
    private final PageCache pageCache;

    BackupStrategyWrapper( BackupStrategy backupStrategy, BackupCopyService backupCopyService, FileSystemAbstraction fs, PageCache pageCache,
            LogProvider logProvider )
    {
        this.backupStrategy = backupStrategy;
        this.backupCopyService = backupCopyService;
        this.fs = fs;
        this.pageCache = pageCache;
        this.log = logProvider.getLog( BackupStrategyWrapper.class );
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
        Path backupLocation = onlineBackupContext.getResolvedLocationFromName();
        OptionalHostnamePort userSpecifiedAddress = onlineBackupContext.getRequiredArguments().getAddress();
        log.debug( "User specified address is %s:%s", userSpecifiedAddress.getHostname().toString(), userSpecifiedAddress.getPort().toString() );
        Config config = onlineBackupContext.getConfig();
        DatabaseLayout backupLayout = DatabaseLayout.of( backupLocation.toFile() );

        boolean previousBackupExists = backupCopyService.backupExists( backupLayout );
        boolean fallbackToFull = onlineBackupContext.getRequiredArguments().isFallbackToFull();

        if ( previousBackupExists )
        {
            log.info( "Previous backup found, trying incremental backup." );
            if ( tryIncrementalBackup( backupLayout, config, userSpecifiedAddress, fallbackToFull ) )
            {
                return;
            }
        }

        if ( fallbackToFull )
        {
            log.info( previousBackupExists
                      ? "Incremental backup failed, a new full backup will be performed."
                      : "Previous backup not found, a new full backup will be performed." );

            fullBackupWithTemporaryFolderResolutions( onlineBackupContext );
        }
        else
        {
            throw new BackupExecutionException( previousBackupExists
                                                ? "Incremental backup failed but full backup is disallowed by configuration"
                                                : "Previous backup does not exist but full backup is disallowed by configuration" );
        }
    }

    private boolean tryIncrementalBackup( DatabaseLayout backupLayout, Config config, OptionalHostnamePort address, boolean fallbackToFullAllowed )
            throws BackupExecutionException
    {
        try
        {
            backupStrategy.performIncrementalBackup( backupLayout, config, address );
            performRecovery( config, backupLayout );
            clearIdFiles( backupLayout );
            return true;
        }
        catch ( Exception e )
        {
            if ( fallbackToFullAllowed )
            {
                log.warn( "Incremental backup failed", e );
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
     * If there is no existing backup, then no renaming will occur.
     * Otherwise the full backup will be done into a temporary directory and renaming
     * will occur if everything was successful.
     * </p>
     *
     * @param onlineBackupContext command line arguments, config etc.
     */
    private void fullBackupWithTemporaryFolderResolutions( OnlineBackupContext onlineBackupContext ) throws BackupExecutionException
    {
        Path userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        Path temporaryFullBackupLocation = backupCopyService.findAnAvailableLocationForNewFullBackup( userSpecifiedBackupLocation );

        OptionalHostnamePort address = onlineBackupContext.getRequiredArguments().getAddress();
        DatabaseLayout backupLayout = DatabaseLayout.of( temporaryFullBackupLocation.toFile() );
        backupStrategy.performFullBackup( backupLayout, onlineBackupContext.getConfig(), address );

        performRecovery( onlineBackupContext.getConfig(), backupLayout );
        clearIdFiles( backupLayout );

        // NOTE temporaryFullBackupLocation can be equal to desired
        boolean backupWasMadeToATemporaryLocation = !userSpecifiedBackupLocation.equals( temporaryFullBackupLocation );
        if ( backupWasMadeToATemporaryLocation )
        {
            renameTemporaryBackupToExpected( temporaryFullBackupLocation, userSpecifiedBackupLocation );
        }
    }

    @VisibleForTesting
    void performRecovery( Config config, DatabaseLayout backupLayout ) throws BackupExecutionException
    {
        try
        {
            Recovery.performRecovery( fs, pageCache, config, backupLayout );
        }
        catch ( IOException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    private void clearIdFiles( DatabaseLayout databaseLayout ) throws BackupExecutionException
    {
        try
        {
            backupCopyService.clearIdFiles( databaseLayout );
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
            Path newBackupLocationForPreExistingBackup = backupCopyService.findNewBackupLocationForBrokenExisting( userSpecifiedBackupLocation );
            backupCopyService.moveBackupLocation( userSpecifiedBackupLocation, newBackupLocationForPreExistingBackup );
            backupCopyService.moveBackupLocation( temporaryFullBackupLocation, userSpecifiedBackupLocation );
        }
        catch ( IOException e )
        {
            throw new BackupExecutionException( e );
        }
    }
}
