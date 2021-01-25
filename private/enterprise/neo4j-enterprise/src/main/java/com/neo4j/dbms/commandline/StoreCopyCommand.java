/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline;

import com.neo4j.dbms.commandline.storeutil.StoreCopy;
import picocli.CommandLine;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.Clocks;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
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
        description = "This command will create a copy of a database.%n" +
                      "If your labels, properties or relationships contain dots or " +
                      "commas you can use ` to escape them, e.g. `My,label`.property "
)
public class StoreCopyCommand extends AbstractCommand
{
    private PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
    @ArgGroup( multiplicity = "1" )
    private SourceOption source = new SourceOption();

    private static class SourceOption
    {
        @Option( names = "--from-database", description = "Name of database to copy from.", required = true, converter = DatabaseNameConverter.class )
        private NormalizedDatabaseName database;
        @Option( names = "--from-path", description = "Path to the database to copy from.", required = true )
        private Path path;
    }

    @Option(
            names = "--from-path-tx",
            description = "Path to the transaction files, if they are not in the same folder as '--from-path'.",
            paramLabel = "<path>"
    )
    private Path sourceTxLogs;

    @Option( names = "--to-database", description = "Name of database to copy to.", required = true, converter = DatabaseNameConverter.class )
    private NormalizedDatabaseName database;

    @Option(
            names = "--neo4j-home-directory",
            description = "Path to the home directory for the copied database. Default is the same as the database copied from.",
            paramLabel = "<path>"
    )
    private Path toHome;

    // --force
    @Option( names = "--force", description = "Force the command to run even if the integrity of the database can not be verified." )
    private boolean force;

    @Option(
            names = "--to-format",
            defaultValue = "same",
            description = "Set the format for the new database. Must be one of ${COMPLETION-CANDIDATES}. 'same' will use the same format as the source. " +
                          "WARNING: If you go from 'high_limit' to 'standard' or 'aligned' there is no validation that the data will actually fit."
    )
    private StoreCopy.FormatEnum format;

    @Option(
            names = "--delete-nodes-with-labels",
            description = "A comma separated list of labels. All nodes that have ANY of the specified labels will be deleted." +
                          "Can not be combined with --keep-only-nodes-with-labels.",
            paramLabel = "<label>[,<label>...]",
            showDefaultValue = NEVER,
            converter = TypeConverter.class
    )
    private List<String> deleteNodesWithLabels = new ArrayList<>();

    @Option(
            names = "--keep-only-nodes-with-labels",
            description = "A comma separated list of labels. All nodes that have ANY of the specified labels will be kept." +
                          "Can not be combined with --delete-nodes-with-labels.",
            paramLabel = "<label>[,<label>...]",
            showDefaultValue = NEVER,
            converter = TypeConverter.class
    )
    private List<String> keepOnlyNodesWithLabels = new ArrayList<>();

    @Option(
            names = "--skip-labels",
            description = "A comma separated list of labels to ignore.",
            paramLabel = "<label>[,<label>...]",
            showDefaultValue = NEVER,
            converter = TypeConverter.class
    )
    private List<String> skipLabels = new ArrayList<>();

    @Option(
            names = "--skip-properties",
            description = "A comma separated list of property keys to ignore. " +
                          "Can not be combined with --skip-node-properties, --keep-only-node-properties, " +
                          "--skip-relationship-properties or --keep-only-relationship-properties.",
            paramLabel = "<property>[,<property>...]",
            showDefaultValue = NEVER,
            converter = TypeConverter.class
    )
    private List<String> skipProperties = new ArrayList<>();

    @Option(
            names = "--skip-node-properties",
            description = "A comma separated list of property keys to ignore for nodes with the specified label. " +
                          "Can not be combined with --skip-properties or --keep-only-node-properties.",
            paramLabel = "<label.property>[,<label.property>...]",
            showDefaultValue = NEVER,
            converter = ComboTypeConverter.class
    )
    private List<List<String>> skipNodeProperties = new ArrayList<>();

    @Option(
            names = "--keep-only-node-properties",
            description = "A comma separated list of property keys to keep for nodes with the specified label. " +
                          "Any labels not explicitly mentioned will keep their properties. " +
                          "Can not be combined with --skip-properties or --skip-node-properties.",
            paramLabel = "<label.property>[,<label.property>...]",
            showDefaultValue = NEVER,
            converter = ComboTypeConverter.class
    )
    private List<List<String>> keepOnlyNodeProperties = new ArrayList<>();

    @Option(
            names = "--skip-relationship-properties",
            description = "A comma separated list of property keys to ignore for relationships with the specified type. " +
                          "Can not be combined with --skip-properties or --keep-only-relationship-properties.",
            paramLabel = "<relationship.property>[,<relationship.property>...]",
            showDefaultValue = NEVER,
            converter = ComboTypeConverter.class
    )
    private List<List<String>> skipRelationshipProperties = new ArrayList<>();

    @Option(
            names = "--keep-only-relationship-properties",
            description = "A comma separated list of property keys to keep for relationships with the specified type. " +
                          "Any relationship types not explicitly mentioned will keep their properties. " +
                          "Can not be combined with --skip-properties or --skip-relationship-properties.",
            paramLabel = "<relationship.property>[,<relationship.property>...]",
            showDefaultValue = NEVER,
            converter = ComboTypeConverter.class
    )
    private List<List<String>> keepOnlyRelationshipProperties = new ArrayList<>();

    @Option(
            names = "--skip-relationships",
            description = "A comma separated list of relationships to ignore.",
            paramLabel = "<relationship>[,<relationship>...]",
            showDefaultValue = NEVER,
            converter = TypeConverter.class
    )
    private List<String> skipRelationships = new ArrayList<>();

    @Option( names = "--from-pagecache", paramLabel = "<size>", defaultValue = "8m", description = "The size of the page cache to use for reading." )
    private String fromPageCacheMemory;

    @Option( names = "--to-pagecache", paramLabel = "<size>", description = "(Advanced) The size of the page cache to use for writing. " +
            "If not specified then an optimal size will be automatically selected" )
    private String toPageCacheMemory;

    public StoreCopyCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute() throws Exception
    {
        verifyCommandLineArguments();

        Config config = buildConfig();
        DatabaseLayout fromDatabaseLayout = getFromDatabaseLayout( config );

        validateSource( fromDatabaseLayout );

        Neo4jLayout toLayout = toHome != null ? Neo4jLayout.of( toHome ) : Neo4jLayout.of( config );
        DatabaseLayout toDatabaseLayout = toLayout.databaseLayout( database.name() );
        var memoryTracker = EmptyMemoryTracker.INSTANCE;

        validateTarget( toDatabaseLayout );

        try ( Closeable ignored = LockChecker.checkDatabaseLock( fromDatabaseLayout ) )
        {
            if ( !force )
            {
                checkDbState( fromDatabaseLayout, config, memoryTracker );
            }
            try ( Closeable ignored2 = LockChecker.checkDatabaseLock( toDatabaseLayout ) )
            {
                StoreCopy copy = new StoreCopy( fromDatabaseLayout, config, format, deleteNodesWithLabels, keepOnlyNodesWithLabels, skipLabels,
                                                skipProperties, skipNodeProperties, keepOnlyNodeProperties, skipRelationshipProperties,
                                                keepOnlyRelationshipProperties,
                                                skipRelationships, verbose, ctx.out(), pageCacheTracer, Clocks.nanoClock() );
                try
                {
                    copy.copyTo( toDatabaseLayout, fromPageCacheMemory, toPageCacheMemory );
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

    private void verifyCommandLineArguments()
    {
        if ( !deleteNodesWithLabels.isEmpty() && !keepOnlyNodesWithLabels.isEmpty() )
        {
            throw new CommandFailedException( "--delete-nodes-with-labels and --keep-only-nodes-with-labels can not be combined" );
        }

        if ( (!skipProperties.isEmpty() && (!skipNodeProperties.isEmpty() || !keepOnlyNodeProperties.isEmpty()))
             || (!skipNodeProperties.isEmpty() && !keepOnlyNodeProperties.isEmpty()) )
        {
            throw new CommandFailedException( "--skip-properties, --skip-node-properties and --keep-only-node-properties can not be combined" );
        }

        if ( (!skipProperties.isEmpty() && (!skipRelationshipProperties.isEmpty() || !keepOnlyRelationshipProperties.isEmpty()))
             || (!skipRelationshipProperties.isEmpty() && !keepOnlyRelationshipProperties.isEmpty()) )
        {
            throw new CommandFailedException( "--skip-properties, --skip-relationship-properties and --keep-only-relationship-properties can not be combined" );
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
            if ( new TransactionLogFilesHelper( ctx.fs(), source.path ).getMatchedFiles().length > 0 )
            {
                // Transaction logs are in the same directory
                return DatabaseLayout.ofFlat( source.path );
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
            return Neo4jLayout.of( config ).databaseLayout( source.database.name() );
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
        Path targetFile = toDatabaseLayout.databaseDirectory();
        if ( Files.exists( targetFile ) )
        {
            if ( Files.isDirectory( targetFile ) )
            {
                try
                {
                    if ( !FileUtils.isDirectoryEmpty( targetFile ) )
                    {
                        throw new CommandFailedException( "The directory is not empty: " + targetFile.toAbsolutePath() );
                    }
                }
                catch ( IOException e )
                {
                    throw new CommandFailedException( "Error accessing: " + targetFile.toAbsolutePath(), e );
                }
            }
            else
            {
                throw new CommandFailedException( "Specified path is a file: " + targetFile.toAbsolutePath() );
            }
        }
        else
        {
            try
            {
                Files.createDirectories( targetFile );
            }
            catch ( IOException e )
            {
                throw new CommandFailedException( "Unable to create directory: " + targetFile.toAbsolutePath() );
            }
        }
    }

    private static void checkDbState( DatabaseLayout databaseLayout, Config additionalConfiguration, MemoryTracker memoryTracker )
    {
        if ( checkRecoveryState( databaseLayout, additionalConfiguration, memoryTracker ) )
        {
            throw new CommandFailedException( joinAsLines( "The database " + databaseLayout.getDatabaseName() + "  was not shut down properly.",
                                                           "Please perform a recovery by starting and stopping the database.",
                                                           "If recovery is not possible, you can force the command to continue with the '--force' flag." ) );
        }
    }

    private static boolean checkRecoveryState( DatabaseLayout databaseLayout, Config additionalConfiguration, MemoryTracker memoryTracker )
    {
        try
        {
            return isRecoveryRequired( databaseLayout, additionalConfiguration, memoryTracker );
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
        cfg.set( record_format, "" ); // Record format should be ignored to allow upgrade
        return cfg;
    }

    @VisibleForTesting
    public void setPageCacheTracer( PageCacheTracer pageCacheTracer )
    {
        this.pageCacheTracer = pageCacheTracer;
    }

    static List<String> quoteAwareSplit( String value, char splitChar, boolean trim )
    {
        List<String> split = new ArrayList<>();

        boolean insideQuote = false;
        int startOfString = 0;
        int argLength = value.length();
        for ( int i = 0; i < argLength; i++ )
        {
            char c = value.charAt( i );
            if ( c == '`' )
            {
                insideQuote = !insideQuote;
            }
            else if ( c == splitChar && !insideQuote )
            {
                split.add( trimQuotesAndCheckString( trim, value.substring( startOfString, i ) ) );
                startOfString = i + 1;
            }
        }
        if ( insideQuote )
        {
            throw new CommandLine.TypeConversionException( "Invalid format: uneven number of back-ticks" );
        }
        if ( startOfString <= argLength )
        {
            split.add( trimQuotesAndCheckString( trim, value.substring( startOfString, argLength ) ) );
        }
        return split;
    }

    private static String trimQuotesAndCheckString( boolean trim, String string )
    {
        if ( trim )
        {
            if ( string.contains( "`" ) && string.length() >= 2 )
            {
                string = string.substring( 1, string.length() - 1 );
            }
            if ( string.contains( "`" ) )
            {
                throw new CommandLine.TypeConversionException( "Invalid format: wrong use of back-ticks" );
            }
        }
        if ( string.isEmpty() )
        {
            throw new CommandLine.TypeConversionException( "Invalid format: Empty entity" );
        }
        return string;
    }

    static class TypeConverter implements CommandLine.ITypeConverter<List<String>>
    {
        @Override
        public List<String> convert( String value )
        {
            return quoteAwareSplit( value, ',', true );
        }
    }

    static class ComboTypeConverter implements CommandLine.ITypeConverter<List<List<String>>>
    {
        @Override
        public List<List<String>> convert( String value )
        {
            return quoteAwareSplit( value, ',', false ).stream()
                                                       .map( s -> quoteAwareSplit( s, '.', true ) ).collect( Collectors.toList() );
        }
    }
}
