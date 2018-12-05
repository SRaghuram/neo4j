/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.File;
import java.nio.file.Path;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Controls the outcome of the backup tool.
 * Iterates over multiple backup strategies and stops when a backup was successful, there was a critical failure or
 * when none of the backups worked.
 * Also handles the consistency check
 */
class BackupStrategyCoordinator
{
    private static final int STATUS_CONSISTENCY_CHECK_ERROR = 2;
    private static final int STATUS_CONSISTENCY_CHECK_INCONSISTENT = 3;

    private final FileSystemAbstraction fs;
    private final ConsistencyCheckService consistencyCheckService;
    private final LogProvider logProvider;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final BackupStrategyWrapper strategy;

    BackupStrategyCoordinator( FileSystemAbstraction fs, ConsistencyCheckService consistencyCheckService, LogProvider logProvider,
            ProgressMonitorFactory progressMonitorFactory, BackupStrategyWrapper strategy )
    {
        this.fs = fs;
        this.consistencyCheckService = consistencyCheckService;
        this.logProvider = logProvider;
        this.progressMonitorFactory = progressMonitorFactory;
        this.strategy = strategy;
    }

    /**
     * Iterate over all the provided strategies trying to perform a successful backup.
     * Will also do consistency checks if specified in {@link OnlineBackupContext}
     *
     * @param onlineBackupContext filesystem, command arguments and configuration
     * @throws CommandFailed when backup failed or there were issues with consistency checks
     */
    public void performBackup( OnlineBackupContext onlineBackupContext ) throws CommandFailed
    {
        OnlineBackupRequiredArguments requiredArgs = onlineBackupContext.getRequiredArguments();
        Path destination = onlineBackupContext.getResolvedLocationFromName();
        ConsistencyFlags consistencyFlags = onlineBackupContext.getConsistencyFlags();

        verifyArguments( onlineBackupContext );

        try
        {
            strategy.doBackup( onlineBackupContext );
        }
        catch ( BackupExecutionException e )
        {
            throw new CommandFailed( "Execution of backup failed", e.getCause() != null ? e.getCause() : e );
        }
        catch ( Throwable t )
        {
            throw new CommandFailed( "Execution of backup failed", t );
        }

        if ( requiredArgs.isDoConsistencyCheck() )
        {
            performConsistencyCheck( onlineBackupContext.getConfig(), requiredArgs, consistencyFlags, DatabaseLayout.of( destination.toFile() ) );
        }
    }

    private void performConsistencyCheck(
            Config config, OnlineBackupRequiredArguments requiredArgs, ConsistencyFlags consistencyFlags,
            DatabaseLayout layout ) throws CommandFailed
    {
        try
        {
            boolean verbose = false;
            File reportDir = requiredArgs.getReportDir().toFile();
            ConsistencyCheckService.Result ccResult = consistencyCheckService.runFullConsistencyCheck(
                    layout,
                    config,
                    progressMonitorFactory,
                    logProvider,
                    fs,
                    verbose,
                    reportDir,
                    consistencyFlags );

            if ( !ccResult.isSuccessful() )
            {
                throw new CommandFailed( format( "Inconsistencies found. See '%s' for details.", ccResult.reportFile() ),
                        STATUS_CONSISTENCY_CHECK_INCONSISTENT );
            }
        }
        catch ( Throwable e )
        {
            if ( e instanceof CommandFailed )
            {
                throw (CommandFailed) e;
            }
            throw new CommandFailed( "Failed to do consistency check on backup: " + e.getMessage(), e, STATUS_CONSISTENCY_CHECK_ERROR );
        }
    }

    private void verifyArguments( OnlineBackupContext onlineBackupContext ) throws CommandFailed
    {
        // Make sure destination exists
        checkDestination( onlineBackupContext.getRequiredArguments().getDirectory() );
        checkDestination( onlineBackupContext.getRequiredArguments().getReportDir() );
    }

    private void checkDestination( Path path ) throws CommandFailed
    {
        if ( !fs.isDirectory( path.toFile() ) )
        {
            throw new CommandFailed( format( "Directory '%s' does not exist.", path ) );
        }
    }
}
