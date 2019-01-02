/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_warmup_enabled;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.impl.util.Converters.toOptionalHostnamePortFromRawAddress;

class OnlineBackupContextFactory
{
    static final String ARG_NAME_BACKUP_DIRECTORY = "backup-dir";
    static final String ARG_DESC_BACKUP_DIRECTORY = "Directory to place backup in.";

    static final String ARG_NAME_BACKUP_NAME = "name";
    static final String ARG_DESC_BACKUP_NAME =
            "Name of backup. If a backup with this name already exists an incremental backup will be attempted.";

    static final String ARG_NAME_BACKUP_SOURCE = "from";
    static final String ARG_DESC_BACKUP_SOURCE = "Host and port of Neo4j.";
    static final String ARG_DFLT_BACKUP_SOURCE = "localhost:6362";

    static final String ARG_NAME_DATABASE_NAME = "database";
    static final String ARG_DESC_DATABASE_NAME = "Name of the remote database to backup.";
    static final String ARG_DFLT_DATABASE_NAME = null;

    static final String ARG_NAME_PAGECACHE = "pagecache";
    static final String ARG_DESC_PAGECACHE = "The size of the page cache to use for the backup process.";
    static final String ARG_DFLT_PAGECACHE = "8m";

    static final String ARG_NAME_REPORT_DIRECTORY = "cc-report-dir";
    static final String ARG_DESC_REPORT_DIRECTORY = "Directory where consistency report will be written.";

    static final String ARG_NAME_ADDITIONAL_CONFIG_DIR = "additional-config";
    static final String ARG_DESC_ADDITIONAL_CONFIG_DIR =
            "Configuration file to supply additional configuration in. This argument is DEPRECATED.";

    static final String ARG_NAME_FALLBACK_FULL = "fallback-to-full";
    static final String ARG_DESC_FALLBACK_FULL =
            "If an incremental backup fails backup will move the old backup to <name>.err.<N> and fallback to a full " +
            "backup instead.";

    static final String ARG_NAME_CHECK_CONSISTENCY = "check-consistency";
    static final String ARG_DESC_CHECK_CONSISTENCY = "If a consistency check should be made.";

    static final String ARG_NAME_CHECK_GRAPH = "cc-graph";
    static final String ARG_DESC_CHECK_GRAPH =
            "Perform consistency checks between nodes, relationships, properties, types and tokens.";

    static final String ARG_NAME_CHECK_INDEXES = "cc-indexes";
    static final String ARG_DESC_CHECK_INDEXES = "Perform consistency checks on indexes.";

    static final String ARG_NAME_CHECK_LABELS = "cc-label-scan-store";
    static final String ARG_DESC_CHECK_LABELS = "Perform consistency checks on the label scan store.";

    static final String ARG_NAME_CHECK_OWNERS = "cc-property-owners";
    static final String ARG_DESC_CHECK_OWNERS =
            "Perform additional consistency checks on property ownership. This check is *very* expensive in time and " +
            "memory.";

    private final Path homeDir;
    private final Path configDir;

    OnlineBackupContextFactory( Path homeDir, Path configDir )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
    }

    public static Arguments arguments()
    {
        return new Arguments()
                .withArgument( new MandatoryCanonicalPath(
                        ARG_NAME_BACKUP_DIRECTORY, "backup-path", ARG_DESC_BACKUP_DIRECTORY ) )
                .withArgument( new MandatoryNamedArg(
                        ARG_NAME_BACKUP_NAME, "graph.db-backup", ARG_DESC_BACKUP_NAME ) )
                .withArgument( new OptionalNamedArg(
                        ARG_NAME_BACKUP_SOURCE, "address", ARG_DFLT_BACKUP_SOURCE, ARG_DESC_BACKUP_SOURCE ) )
                .withArgument( new OptionalNamedArg(
                        ARG_NAME_DATABASE_NAME, "graph.db", ARG_DFLT_DATABASE_NAME, ARG_DESC_DATABASE_NAME ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_FALLBACK_FULL, true, ARG_DESC_FALLBACK_FULL ) )
                .withArgument( new OptionalNamedArg(
                        ARG_NAME_PAGECACHE, "8m", ARG_DFLT_PAGECACHE, ARG_DESC_PAGECACHE ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_CONSISTENCY, true, ARG_DESC_CHECK_CONSISTENCY ) )
                .withArgument( new OptionalCanonicalPath(
                        ARG_NAME_REPORT_DIRECTORY, "directory", ".", ARG_DESC_REPORT_DIRECTORY ) )
                .withArgument( new OptionalCanonicalPath(
                        ARG_NAME_ADDITIONAL_CONFIG_DIR, "config-file-path", "", ARG_DESC_ADDITIONAL_CONFIG_DIR ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_GRAPH, true, ARG_DESC_CHECK_GRAPH ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_INDEXES, true, ARG_DESC_CHECK_INDEXES ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_LABELS, true, ARG_DESC_CHECK_LABELS ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_OWNERS, false, ARG_DESC_CHECK_OWNERS ) );
    }

    public OnlineBackupContext createContext( String... args ) throws IncorrectUsage, CommandFailed
    {
        try
        {
            Arguments arguments = arguments();
            arguments.parse( args );

            OptionalHostnamePort address = toOptionalHostnamePortFromRawAddress( arguments.get( ARG_NAME_BACKUP_SOURCE ) );

            Path backupDirectory = getBackupDirectory( arguments );
            String backupName = arguments.get( ARG_NAME_BACKUP_NAME );
            Path logPath = backupDirectory.resolve( backupName );

            String pageCacheMemory = arguments.get( ARG_NAME_PAGECACHE );
            Optional<Path> additionalConfig = arguments.getOptionalPath( ARG_NAME_ADDITIONAL_CONFIG_DIR );

            Path configFile = configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME );
            Config.Builder builder = Config.fromFile( configFile );
            Config config = builder.withHome( homeDir )
                                   .withSetting( transaction_logs_root_path, logPath.toString() )
                                   .withConnectorsDisabled()
                                   .withNoThrowOnFileLoadFailure() // Online backup does not require the presence of a neo4j.conf file.
                                   .build();
            additionalConfig.map( this::loadAdditionalConfigFile ).ifPresent( config::augment );

            // We replace the page cache memory setting.
            // Any other custom page swapper, etc. settings are preserved and used.
            config.augment( pagecache_memory, pageCacheMemory );
            // warmup is also disabled because it is not needed for temporary databases
            config.augment( pagecache_warmup_enabled.name(), Settings.FALSE );

            // Disable all metrics to avoid port binding and JMX naming exceptions
            config.augment( "metrics.enabled", Settings.FALSE );

            return OnlineBackupContext.builder()
                    .withHostnamePort( address )
                    .withDatabaseName( arguments.get( ARG_NAME_DATABASE_NAME ) )
                    .withBackupName( backupName )
                    .withBackupDirectory( backupDirectory )
                    .withFallbackToFullBackup( arguments.getBoolean( ARG_NAME_FALLBACK_FULL ) )
                    .withReportsDirectory( getReportDirectory( arguments ) )
                    .withConsistencyCheck( arguments.getBoolean( ARG_NAME_CHECK_CONSISTENCY ) )
                    .withConsistencyCheckGraph( getBoolean( arguments, ARG_NAME_CHECK_GRAPH ) )
                    .withConsistencyCheckIndexes( getBoolean( arguments, ARG_NAME_CHECK_INDEXES ) )
                    .withConsistencyCheckLabelScanStore( getBoolean( arguments, ARG_NAME_CHECK_LABELS ) )
                    .withConsistencyCheckPropertyOwners( getBoolean( arguments, ARG_NAME_CHECK_OWNERS ) )
                    .withConfig( config )
                    .build();
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        catch ( UncheckedIOException e )
        {
            throw new CommandFailed( e.getMessage(), e );
        }
    }

    private Path getBackupDirectory( Arguments arguments ) throws CommandFailed
    {
        Path path = arguments.getMandatoryPath( ARG_NAME_BACKUP_DIRECTORY );
        try
        {
            return path.toRealPath();
        }
        catch ( IOException e )
        {
            throw new CommandFailed( String.format( "Directory '%s' does not exist.", path ) );
        }
    }

    private Path getReportDirectory( Arguments arguments )
    {
        return arguments.getOptionalPath( ARG_NAME_REPORT_DIRECTORY )
                .orElseThrow( () -> new IllegalArgumentException( ARG_NAME_REPORT_DIRECTORY + " must be a path" ) );
    }

    private Boolean getBoolean( Arguments arguments, String argName )
    {
        return arguments.has( argName ) ? arguments.getBoolean( argName ) : null;
    }

    private Config loadAdditionalConfigFile( Path path )
    {
        return Config.fromFile( path ).build();
    }
}
