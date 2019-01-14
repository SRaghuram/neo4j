/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;

import static java.lang.String.format;
import static org.neo4j.backup.impl.SelectedBackupProtocol.CATCHUP;

class OnlineBackupCommand implements AdminCommand
{
    private final OutsideWorld outsideWorld;
    private final OnlineBackupContextFactory contextBuilder;
    private final BackupStrategyCoordinatorFactory backupStrategyCoordinatorFactory;
    private final BackupSupportingClassesFactory backupSupportingClassesFactory;

    /**
     * The entry point for neo4j admin tool's online backup functionality.
     *
     * @param outsideWorld provides a way to interact with the filesystem and output streams
     * @param contextBuilder helper class to validate, process and return a grouped result of processing the command line arguments
     * @param backupSupportingClassesFactory necessary for constructing the strategy for backing up over the causal clustering transaction protocol
     * @param backupStrategyCoordinatorFactory class that actually handles the logic of performing a backup
     */
    OnlineBackupCommand( OutsideWorld outsideWorld, OnlineBackupContextFactory contextBuilder,
                         BackupSupportingClassesFactory backupSupportingClassesFactory,
                         BackupStrategyCoordinatorFactory backupStrategyCoordinatorFactory )
    {
        this.outsideWorld = outsideWorld;
        this.contextBuilder = contextBuilder;
        this.backupSupportingClassesFactory = backupSupportingClassesFactory;
        this.backupStrategyCoordinatorFactory = backupStrategyCoordinatorFactory;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        OnlineBackupContext onlineBackupContext = contextBuilder.createContext( args );
        validateArguments( onlineBackupContext.getRequiredArguments() );
        try ( BackupSupportingClasses backupSupportingClasses = backupSupportingClassesFactory.createSupportingClasses( onlineBackupContext ) )
        {
            // Make sure destination exists
            checkDestination( onlineBackupContext.getRequiredArguments().getDirectory() );
            checkDestination( onlineBackupContext.getRequiredArguments().getReportDir() );

            BackupStrategyCoordinator backupStrategyCoordinator =
                    backupStrategyCoordinatorFactory.backupStrategyCoordinator( onlineBackupContext, backupSupportingClasses.getBackupProtocolService(),
                            backupSupportingClasses.getBackupDelegator(), backupSupportingClasses.getPageCache() );

            backupStrategyCoordinator.performBackup( onlineBackupContext );
            outsideWorld.stdOutLine( "Backup complete." );
        }
    }

    private void validateArguments( OnlineBackupRequiredArguments arguments ) throws IncorrectUsage
    {
        checkProtocolCompatibilityWithMultiDatabase( arguments );
        informAboutProtocolApplicability( arguments );
    }

    /**
     * Remote database names cannot be specified in the common protocol.
     */
    private void checkProtocolCompatibilityWithMultiDatabase( OnlineBackupRequiredArguments arguments ) throws IncorrectUsage
    {
        if ( arguments.getDatabaseName().isPresent() )
        {
            if ( !arguments.getSelectedBackupProtocol().equals( CATCHUP ) )
            {
                throw new IncorrectUsage( "Only the catchup protocol supports specifying the remote database name." );
            }
        }
    }

    /**
     * A soft warning to users of the backup client.
     */
    private void informAboutProtocolApplicability( OnlineBackupRequiredArguments arguments )
    {
        SelectedBackupProtocol selectedBackupProtocol = arguments.getSelectedBackupProtocol();
        if ( !SelectedBackupProtocol.ANY.equals( selectedBackupProtocol ) )
        {
            final String compatibleProducts;
            switch ( selectedBackupProtocol )
            {
            case CATCHUP:
                compatibleProducts = "causal clustering";
                break;
            case COMMON:
                compatibleProducts = "HA and single";
                break;
            default:
                throw new IllegalArgumentException( "Unhandled protocol " + selectedBackupProtocol );
            }
            outsideWorld.stdOutLine( format( "The selected protocol `%s` means that it is only compatible with %s instances", selectedBackupProtocol.getName(),
                    compatibleProducts ) );
        }
    }

    private void checkDestination( Path path ) throws CommandFailed
    {
        if ( !outsideWorld.fileSystem().isDirectory( path.toFile() ) )
        {
            throw new CommandFailed( format( "Directory '%s' does not exist.", path ) );
        }
    }
}
