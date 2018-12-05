/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;

class OnlineBackupCommand implements AdminCommand
{
    private static final int STATUS_CONSISTENCY_CHECK_ERROR = 2;
    private static final int STATUS_CONSISTENCY_CHECK_INCONSISTENT = 3;

    private final OutsideWorld outsideWorld;
    private final OnlineBackupContextFactory contextBuilder;
    private final OnlineBackupExecutor backupExecutor;

    /**
     * The entry point for neo4j admin tool's online backup functionality.
     *
     * @param outsideWorld provides a way to interact with the filesystem and output streams
     * @param contextBuilder helper class to validate, process and return a grouped result of processing the command line arguments
     * @param backupExecutor helper that executes backup according to the given context
     */
    OnlineBackupCommand( OutsideWorld outsideWorld, OnlineBackupContextFactory contextBuilder, OnlineBackupExecutor backupExecutor )
    {
        this.outsideWorld = outsideWorld;
        this.contextBuilder = contextBuilder;
        this.backupExecutor = backupExecutor;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        OnlineBackupContext onlineBackupContext = contextBuilder.createContext( args );

        try
        {
            backupExecutor.executeBackup( onlineBackupContext );
        }
        catch ( BackupExecutionException e )
        {
            throw new CommandFailed( "Execution of backup failed", e.getCause() != null ? e.getCause() : e );
        }
        catch ( ConsistencyCheckExecutionException e )
        {
            int exitCode = e.consistencyCheckFailedToExecute() ? STATUS_CONSISTENCY_CHECK_ERROR : STATUS_CONSISTENCY_CHECK_INCONSISTENT;
            throw new CommandFailed( e.getMessage(), e.getCause(), exitCode );
        }
        catch ( Exception e )
        {
            throw new CommandFailed( "Execution of backup failed", e );
        }

        outsideWorld.stdOutLine( "Backup complete." );
    }
}
