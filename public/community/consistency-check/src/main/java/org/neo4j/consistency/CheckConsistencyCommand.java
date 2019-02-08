/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static java.lang.String.format;
import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;

public class CheckConsistencyCommand implements AdminCommand
{
    public static final String CHECK_GRAPH = "check-graph";
    public static final String CHECK_INDEXES = "check-indexes";
    public static final String CHECK_LABEL_SCAN_STORE = "check-label-scan-store";
    public static final String CHECK_PROPERTY_OWNERS = "check-property-owners";
    private static final Arguments arguments = new Arguments()
            .withDatabase()
            .withArgument( new OptionalCanonicalPath( "backup", "/path/to/backup", "",
                    "Path to backup to check consistency of. Cannot be used together with --database." ) )
            .withArgument( new OptionalBooleanArg( "verbose", false, "Enable verbose output." ) )
            .withArgument( new OptionalCanonicalPath( "report-dir", "directory", ".",
                    "Directory to write report file in." ) )
            .withArgument( new OptionalCanonicalPath( "additional-config", "config-file-path", "",
                    "Configuration file to supply additional configuration in. This argument is DEPRECATED." ) )
            .withArgument( new OptionalBooleanArg( CHECK_GRAPH, true,
                    "Perform checks between nodes, relationships, properties, types and tokens." ) )
            .withArgument( new OptionalBooleanArg( CHECK_INDEXES, true,
                    "Perform checks on indexes." ) )
            .withArgument( new OptionalBooleanArg( CHECK_LABEL_SCAN_STORE, true,
                    "Perform checks on the label scan store." ) )
            .withArgument( new OptionalBooleanArg( CHECK_PROPERTY_OWNERS, false,
                    "Perform additional checks on property ownership. This check is *very* expensive in time and " +
                            "memory." ) );

    private final Path homeDir;
    private final Path configDir;
    private final ConsistencyCheckService consistencyCheckService;

    public CheckConsistencyCommand( Path homeDir, Path configDir )
    {
        this( homeDir, configDir, new ConsistencyCheckService() );
    }

    public CheckConsistencyCommand( Path homeDir, Path configDir, ConsistencyCheckService consistencyCheckService )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.consistencyCheckService = consistencyCheckService;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        final String database;
        final boolean verbose;
        final Optional<Path> additionalConfigFile;
        final Path reportDir;
        final Optional<Path> backupPath;
        final boolean checkGraph;
        final boolean checkIndexes;
        final boolean checkLabelScanStore;
        final boolean checkPropertyOwners;

        try
        {
            database = arguments.parse( args ).get( ARG_DATABASE );
            backupPath = arguments.getOptionalPath( "backup" );
            verbose = arguments.getBoolean( "verbose" );
            additionalConfigFile = arguments.getOptionalPath( "additional-config" );
            reportDir = arguments.getOptionalPath( "report-dir" )
                    .orElseThrow( () -> new IllegalArgumentException( "report-dir must be a valid path" ) );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        if ( backupPath.isPresent() )
        {
            if ( arguments.has( ARG_DATABASE ) )
            {
                throw new IncorrectUsage( "Only one of '--" + ARG_DATABASE + "' and '--backup' can be specified." );
            }
            if ( !backupPath.get().toFile().isDirectory() )
            {
                throw new CommandFailed( format( "Specified backup should be a directory: %s", backupPath.get() ) );
            }
        }

        Config config = loadNeo4jConfig( homeDir, configDir, database, loadAdditionalConfig( additionalConfigFile ) );

        try
        {
            // We can remove the loading from config file in 4.0
            if ( arguments.has( CHECK_GRAPH ) )
            {
                checkGraph = arguments.getBoolean( CHECK_GRAPH );
            }
            else
            {
                checkGraph = config.get( ConsistencyCheckSettings.consistency_check_graph );
            }
            if ( arguments.has( CHECK_INDEXES ) )
            {
                checkIndexes = arguments.getBoolean( CHECK_INDEXES );
            }
            else
            {
                checkIndexes = config.get( ConsistencyCheckSettings.consistency_check_indexes );
            }
            if ( arguments.has( CHECK_LABEL_SCAN_STORE ) )
            {
                checkLabelScanStore = arguments.getBoolean( CHECK_LABEL_SCAN_STORE );
            }
            else
            {
                checkLabelScanStore = config.get( ConsistencyCheckSettings.consistency_check_label_scan_store );
            }
            if ( arguments.has( CHECK_PROPERTY_OWNERS ) )
            {
                checkPropertyOwners = arguments.getBoolean( CHECK_PROPERTY_OWNERS );
            }
            else
            {
                checkPropertyOwners = config.get( ConsistencyCheckSettings.consistency_check_property_owners );
            }
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            File databaseDirectory = backupPath.map( Path::toFile ).orElse( config.get( database_path ) );
            DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDirectory );
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine( Service.load( StorageEngineFactory.class ) );
            checkDbState( databaseLayout, config, storageEngineFactory );
            ZoneId logTimeZone = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            // Only output progress indicator if a console receives the output
            ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.NONE;
            if ( System.console() != null )
            {
                progressMonitorFactory = ProgressMonitorFactory.textual( System.out );
            }

            ConsistencyCheckService.Result consistencyCheckResult = consistencyCheckService
                    .runFullConsistencyCheck( databaseLayout, config, progressMonitorFactory,
                            FormattedLogProvider.withZoneId( logTimeZone ).toOutputStream( System.out ), fileSystem,
                            verbose, reportDir.toFile(),
                            new ConsistencyFlags( checkGraph, checkIndexes, checkLabelScanStore, checkPropertyOwners ) );

            if ( !consistencyCheckResult.isSuccessful() )
            {
                throw new CommandFailed( format( "Inconsistencies found. See '%s' for details.",
                        consistencyCheckResult.reportFile() ) );
            }
        }
        catch ( ConsistencyCheckIncompleteException | IOException e )
        {
            throw new CommandFailed( "Consistency checking failed." + e.getMessage(), e );
        }
    }

    private static Config loadAdditionalConfig( Optional<Path> additionalConfigFile )
    {
        if ( additionalConfigFile.isPresent() )
        {
            try
            {
                return Config.fromFile( additionalConfigFile.get() ).build();
            }
            catch ( Exception e )
            {
                throw new IllegalArgumentException(
                        String.format( "Could not read configuration file [%s]", additionalConfigFile ), e );
            }
        }

        return Config.defaults();
    }

    private static void checkDbState( DatabaseLayout databaseLayout, Config additionalConfiguration, StorageEngineFactory storageEngineFactory )
            throws CommandFailed
    {
        if ( checkRecoveryState( databaseLayout, additionalConfiguration, storageEngineFactory ) )
        {
            throw new CommandFailed( joinAsLines( "Active logical log detected, this might be a source of inconsistencies.",
                    "Please recover database before running the consistency check.",
                    "To perform recovery please start database and perform clean shutdown." ) );
        }
    }

    private static boolean checkRecoveryState( DatabaseLayout databaseLayout, Config additionalConfiguration, StorageEngineFactory storageEngineFactory )
            throws CommandFailed
    {
        try
        {
            return isRecoveryRequired( databaseLayout, additionalConfiguration, storageEngineFactory );
        }
        catch ( Exception e )
        {
            throw new CommandFailed( "Failure when checking for recovery state: '%s'." + e.getMessage(), e );
        }
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName, Config additionalConfig )
    {
        Config config = Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withConnectorsDisabled()
                .withNoThrowOnFileLoadFailure()
                .build();
        config.augment( additionalConfig );
        config.augment( GraphDatabaseSettings.active_database, databaseName );
        return config;
    }

    public static Arguments arguments()
    {
        return arguments;
    }
}
