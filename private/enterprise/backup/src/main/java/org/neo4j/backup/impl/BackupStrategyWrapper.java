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
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.recovery.Recovery.performRecovery;

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
    private final Config config;

    BackupStrategyWrapper( BackupStrategy backupStrategy, BackupCopyService backupCopyService, FileSystemAbstraction fs, PageCache pageCache, Config config,
            LogProvider logProvider )
    {
        this.backupStrategy = backupStrategy;
        this.backupCopyService = backupCopyService;
        this.fs = fs;
        this.pageCache = pageCache;
        this.config = config;
        this.log = logProvider.getLog( BackupStrategyWrapper.class );
    }

    /**
     * Try to do a backup using the given strategy (ex. BackupProtocol). This covers all stages (starting with incremental and falling back to a a full backup).
     * The end result of this method will either be a successful backup or any other return type with the reason why the backup wasn't successful
     *
     * @param onlineBackupContext the command line arguments, configuration, flags
     * @return the ultimate outcome of trying to do a backup with the given strategy
     */
    Fallible<BackupStrategyOutcome> doBackup( OnlineBackupContext onlineBackupContext )
    {
        LifeSupport lifeSupport = new LifeSupport();
        lifeSupport.add( backupStrategy );
        lifeSupport.start();
        Fallible<BackupStrategyOutcome> state = performBackupWithoutLifecycle( onlineBackupContext );
        lifeSupport.shutdown();
        return state;
    }

    private Fallible<BackupStrategyOutcome> performBackupWithoutLifecycle( OnlineBackupContext onlineBackupContext )
    {
        Path backupLocation = onlineBackupContext.getResolvedLocationFromName();
        OptionalHostnamePort userSpecifiedAddress = onlineBackupContext.getRequiredArguments().getAddress();
        log.debug( "User specified address is %s:%s", userSpecifiedAddress.getHostname().toString(), userSpecifiedAddress.getPort().toString() );
        Config config = onlineBackupContext.getConfig();

        DatabaseLayout backupLayout = DatabaseLayout.of( backupLocation.toFile() );
        boolean previousBackupExists = backupCopyService.backupExists( backupLayout );
        if ( previousBackupExists )
        {
            log.info( "Previous backup found, trying incremental backup." );
            Fallible<BackupStageOutcome> state =
                    backupStrategy.performIncrementalBackup( backupLayout, config, userSpecifiedAddress );
            boolean fullBackupWontWork = BackupStageOutcome.WRONG_PROTOCOL.equals( state.getState() );
            boolean incrementalWasSuccessful = BackupStageOutcome.SUCCESS.equals( state.getState() );
            if ( incrementalWasSuccessful )
            {
                backupRecoveryService.recoverWithDatabase( backupLocation, pageCache, config );
            }

            if ( fullBackupWontWork || incrementalWasSuccessful )
            {
                clearIdFiles( backupLayout );
                return describeOutcome( state );
            }
            if ( !onlineBackupContext.getRequiredArguments().isFallbackToFull() )
            {
                return describeOutcome( state );
            }
        }
        if ( onlineBackupContext.getRequiredArguments().isFallbackToFull() )
        {
            if ( !previousBackupExists )
            {
                log.info( "Previous backup not found, a new full backup will be performed." );
            }
            return describeOutcome( fullBackupWithTemporaryFolderResolutions( onlineBackupContext ) );
        }
        return new Fallible<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null );
    }

    private void clearIdFiles( DatabaseLayout databaseLayout )
    {
        try
        {
            backupCopyService.clearIdFiles( databaseLayout );
        }
        catch ( IOException e )
        {
            log.warn( "Failed to delete some or all id files.", e );
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
     * @return outcome of full backup
     */
    private Fallible<BackupStageOutcome> fullBackupWithTemporaryFolderResolutions( OnlineBackupContext onlineBackupContext )
    {
        Path userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        Path temporaryFullBackupLocation = backupCopyService.findAnAvailableLocationForNewFullBackup( userSpecifiedBackupLocation );

        OptionalHostnamePort address = onlineBackupContext.getRequiredArguments().getAddress();
        DatabaseLayout backupLayout = DatabaseLayout.of( temporaryFullBackupLocation.toFile() );
        Fallible<BackupStageOutcome> state = backupStrategy.performFullBackup( backupLayout, config, address );

        // NOTE temporaryFullBackupLocation can be equal to desired
        boolean backupWasMadeToATemporaryLocation = !userSpecifiedBackupLocation.equals( temporaryFullBackupLocation );

        if ( BackupStageOutcome.SUCCESS.equals( state.getState() ) )
        {
            try
            {
                recoverBackup( backupLayout );
                if ( backupWasMadeToATemporaryLocation )
                {
                    renameTemporaryBackupToExpected( temporaryFullBackupLocation, userSpecifiedBackupLocation );
                }
                clearIdFiles( backupLayout );
            }
            catch ( IOException e )
            {
                return new Fallible<>( BackupStageOutcome.UNRECOVERABLE_FAILURE, e );
            }
        }
        return state;
    }

    void recoverBackup( DatabaseLayout backupLayout ) throws IOException
    {
        performRecovery( fs, pageCache, config, backupLayout );
    }

    private void renameTemporaryBackupToExpected( Path temporaryFullBackupLocation, Path userSpecifiedBackupLocation ) throws IOException
    {
        Path newBackupLocationForPreExistingBackup = backupCopyService.findNewBackupLocationForBrokenExisting( userSpecifiedBackupLocation );
        backupCopyService.moveBackupLocation( userSpecifiedBackupLocation, newBackupLocationForPreExistingBackup );
        backupCopyService.moveBackupLocation( temporaryFullBackupLocation, userSpecifiedBackupLocation );
    }

    private static Fallible<BackupStrategyOutcome> describeOutcome( Fallible<BackupStageOutcome> strategyStageOutcome )
    {
        BackupStageOutcome stageOutcome = strategyStageOutcome.getState();
        switch ( stageOutcome )
        {
        case SUCCESS:
            return new Fallible<>( BackupStrategyOutcome.SUCCESS, null );
        case WRONG_PROTOCOL:
            return new Fallible<>( BackupStrategyOutcome.INCORRECT_STRATEGY, strategyStageOutcome.getCause().orElse( null ) );
        case FAILURE:
            return new Fallible<>( BackupStrategyOutcome.CORRECT_STRATEGY_FAILED, strategyStageOutcome.getCause().orElse( null ) );
        case UNRECOVERABLE_FAILURE:
            return new Fallible<>( BackupStrategyOutcome.ABSOLUTE_FAILURE, strategyStageOutcome.getCause().orElse( null ) );
        default:
            throw new RuntimeException( "Not all enums covered: " + stageOutcome );
        }
    }
}
