/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline;

import com.neo4j.dbms.commandline.storeutil.StoreCopy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;

import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.ArgGroup;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.NEVER;
import static picocli.CommandLine.Option;

@Command(
        name = "copy",
        header = "Copy a database and optionally apply filters.",
        description = "This command will create a copy of a database."
)
public class StoreCopyCommand extends AbstractCommand
{
    @ArgGroup( multiplicity = "1" )
    private SourceOption source = new SourceOption();

    private static class SourceOption
    {
        @Option( names = "--from-database", description = "Name of database to copy from.", required = true )
        private String database;
        @Option( names = "--from-path", description = "Path to the database to copy from.", required = true )
        private Path path;
    }

    @Option(
            names = "--from-path-tx",
            description = "Path to the transaction files, if they are not in the same folder as '--from-path'.",
            paramLabel = "<path>"
    )
    private Path sourceTxLogs;

    @Option( names = "--to-database", description = "Name of database to copy to.", required = true )
    private String database;

    // --force
    @Option( names = "--force", description = "Force the command to run even if the integrity of the database can not be verified." )
    private boolean force;

    @Option(
            names = "--to-format",
            defaultValue = "same",
            description = "Set the format for the new database. Must be one of ${COMPLETION-CANDIDATES}. 'same' will use the same format as the source. " +
                    "WARNING: If you go from 'high_limit' to 'standard' there is no validation that the data will actually fit."
    )
    private StoreCopy.FormatEnum format;

    @Option(
            names = "--delete-nodes-with-labels",
            description = "A comma separated list of labels. All nodes that have ANY of the specified labels will be deleted.",
            split = ",",
            paramLabel = "<label>",
            showDefaultValue = NEVER
    )
    private List<String> deleteNodesWithLabels = new ArrayList<>();

    @Option(
            names = "--skip-labels",
            description = "A comma separated list of labels to ignore.",
            split = ",",
            paramLabel = "<label>",
            showDefaultValue = NEVER
    )
    private List<String> skipLabels = new ArrayList<>();

    @Option(
            names = "--skip-properties",
            description = "A comma separated list of property keys to ignore.",
            split = ",",
            paramLabel = "<property>",
            showDefaultValue = NEVER
    )
    private List<String> skipProperties = new ArrayList<>();

    @Option(
            names = "--skip-relationships",
            description = "A comma separated list of relationships to ignore.",
            split = ",",
            paramLabel = "<relationship>",
            showDefaultValue = NEVER
    )
    private List<String> skipRelationships = new ArrayList<>();

    public StoreCopyCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute() throws Exception
    {
        Config config = buildConfig();
        DatabaseLayout fromDatabaseLayout = getFromDatabaseLayout( config );

        validateSource( fromDatabaseLayout );

        DatabaseLayout toDatabaseLayout = Neo4jLayout.of( config ).databaseLayout( database );

        validateTarget( toDatabaseLayout );

        try ( Closeable ignored = LockChecker.checkDatabaseLock( fromDatabaseLayout ) )
        {
            if ( !force )
            {
                checkDbState( fromDatabaseLayout, config );
            }
            try ( Closeable ignored2 = LockChecker.checkDatabaseLock( toDatabaseLayout )  )
            {
                StoreCopy copy =
                        new StoreCopy( fromDatabaseLayout, config, format, deleteNodesWithLabels, skipLabels, skipProperties, skipRelationships, verbose,
                                ctx.out() );
                try
                {
                    copy.copyTo( toDatabaseLayout );
                }
                catch ( Exception e )
                {
                    throw new CommandFailedException( "There was a problem during copy.", e );
                }
            }
            catch ( FileLockException e )
            {
                throw new CommandFailedException( "Unable to lock destination.", e );
            }
        }
        catch ( FileLockException e )
        {
            throw new CommandFailedException( "The database is in use. Stop database '" + fromDatabaseLayout.getDatabaseName() + "' and try again.", e );
        }
    }

    private DatabaseLayout getFromDatabaseLayout( Config config )
    {
        if ( source.path != null )
        {
            source.path = source.path.toAbsolutePath();
            if ( !Files.isDirectory( source.path ) )
            {
                throw new CommandFailedException( "The path doesn't exist or not a directory: " + source.path );
            }
            if ( new TransactionLogFilesHelper( ctx.fs(), source.path.toFile() ).getLogFiles().length > 0 )
            {
                // Transaction logs are in the same directory
                return DatabaseLayout.ofFlat( source.path.toFile() );
            }
            else
            {
                if ( sourceTxLogs == null )
                {
                    // Transaction logs are in the same directory and not configured, unable to continue
                    throw new CommandFailedException( "Unable to find transaction logs, please specify the location with '--from-path-tx'." );
                }

                Path databaseName = source.path.getFileName();
                if ( !databaseName.equals( sourceTxLogs.getFileName() ) )
                {
                    throw new CommandFailedException( "The directory with data and the directory with transaction logs need to have the same name." );
                }

                sourceTxLogs = sourceTxLogs.toAbsolutePath();
                Config cfg = Config.newBuilder()
                        .set( default_database, databaseName.toString() )
                        .set( neo4j_home, source.path.getParent() )
                        .set( databases_root_path, source.path.getParent() )
                        .set( transaction_logs_root_path, sourceTxLogs.getParent() )
                        .build();
                return DatabaseLayout.of( cfg );
            }
        }
        else
        {
            return Neo4jLayout.of( config ).databaseLayout( source.database );
        }
    }

    private static void validateSource( DatabaseLayout fromDatabaseLayout )
    {
        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( fromDatabaseLayout.databaseDirectory() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new CommandFailedException( "Database does not exist: " + fromDatabaseLayout.getDatabaseName(), e );
        }
    }

    private static void validateTarget( DatabaseLayout toDatabaseLayout )
    {
        File targetFile = toDatabaseLayout.databaseDirectory();
        if ( targetFile.exists() )
        {
            if ( targetFile.isDirectory() )
            {
                String[] files = targetFile.list();
                if ( files == null || files.length > 0 )
                {
                    throw new CommandFailedException( "The directory is not empty: " + targetFile.getAbsolutePath() );
                }
            }
            else
            {
                throw new CommandFailedException( "Specified path is a file: " + targetFile.getAbsolutePath() );
            }
        }
        else
        {
            try
            {
                Files.createDirectories( targetFile.toPath() );
            }
            catch ( IOException e )
            {
                throw new CommandFailedException( "Unable to create directory: " + targetFile.getAbsolutePath() );
            }
        }
    }

    private static void checkDbState( DatabaseLayout databaseLayout, Config additionalConfiguration )
    {
        if ( checkRecoveryState( databaseLayout, additionalConfiguration ) )
        {
            throw new CommandFailedException( joinAsLines( "The database " + databaseLayout.getDatabaseName() + "  was not shut down properly.",
                    "Please perform a recovery by starting and stopping the database.",
                    "If recovery is not possible, you can force the command to continue with the '--force' flag.") );
        }
    }

    private static boolean checkRecoveryState( DatabaseLayout databaseLayout, Config additionalConfiguration )
    {
        try
        {
            return isRecoveryRequired( databaseLayout, additionalConfiguration );
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( "Failure when checking for recovery state: '%s'." + e.getMessage(), e );
        }
    }

    private Config buildConfig()
    {
        Config cfg = Config.newBuilder()
                .fromFileNoThrow( ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .set( GraphDatabaseSettings.neo4j_home, ctx.homeDir() ).build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
    }
}
