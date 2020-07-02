/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import java.nio.file.Path;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
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
     * @param context the backup context.
     * @throws BackupExecutionException when backup failed
     * @throws ConsistencyCheckExecutionException when backup succeeded but consistency check found inconsistencies or failed
     */
    public void performBackup( OnlineBackupContext context ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        Path destination = context.getDatabaseBackupDir();
        ConsistencyFlags consistencyFlags = context.getConsistencyFlags();

        strategy.doBackup( context );

        if ( context.consistencyCheckEnabled() )
        {
            performConsistencyCheck( context, consistencyFlags, DatabaseLayout.ofFlat( destination ) );
        }
    }

    private void performConsistencyCheck(
            OnlineBackupContext context, ConsistencyFlags consistencyFlags,
            DatabaseLayout layout ) throws ConsistencyCheckExecutionException
    {
        ConsistencyCheckService.Result ccResult;
        try
        {
            ccResult = consistencyCheckService.runFullConsistencyCheck(
                    layout,
                    context.getConfig(),
                    progressMonitorFactory,
                    logProvider,
                    fs,
                    false,
                    context.getReportDir(),
                    consistencyFlags );
        }
        catch ( Exception e )
        {
            throw new ConsistencyCheckExecutionException( "Failed to do consistency check on the backup", e, true );
        }

        if ( !ccResult.isSuccessful() )
        {
            throw new ConsistencyCheckExecutionException( format( "Inconsistencies found. See '%s' for details.", ccResult.reportFile() ), false );
        }
    }
}
