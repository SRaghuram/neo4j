/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.v4.metadata.IncludeMetadata;
import com.neo4j.configuration.OnlineBackupSettings;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.Util;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.consistency.ConsistencyCheckOptions;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.logging.Level;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static com.neo4j.configuration.OnlineBackupSettings.DEFAULT_BACKUP_HOST;
import static com.neo4j.configuration.OnlineBackupSettings.DEFAULT_BACKUP_PORT;
import static java.lang.Boolean.FALSE;
import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.neo4j.cli.Converters.DatabaseNamePatternConverter;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_enabled;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(
        name = "backup",
        header = "Perform an online backup from a running Neo4j enterprise server.",
        description = "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup service must " +
                "have been configured on the server beforehand.%n" +
                "%n" +
                "All consistency checks except 'cc-graph' can be quite expensive so it may be useful to turn them off" +
                " for very large databases. Increasing the heap size can also be a good idea." +
                " See 'neo4j-admin help' for details.%n" +
                "%n" +
                "For more information see: https://neo4j.com/docs/operations-manual/current/backup/"
)
public class OnlineBackupCommand extends AbstractCommand
{
    protected static final int STATUS_CONSISTENCY_CHECK_ERROR = 2;
    protected static final int STATUS_CONSISTENCY_CHECK_INCONSISTENT = 3;

    @Option( names = "--backup-dir", paramLabel = "<path>", required = true, description = "Directory to place backup in." )
    private Path backupDir;

    @Option( names = "--from", paramLabel = "<host:port>", defaultValue = DEFAULT_BACKUP_HOST + ":" + DEFAULT_BACKUP_PORT,
            description = "Host and port of Neo4j." )
    private String from;

    @Option( names = "--database", defaultValue = DEFAULT_DATABASE_NAME,
            description = "Name of the remote database to backup. Can contain * and ? for globbing.",
            converter = DatabaseNamePatternConverter.class )
    private DatabaseNamePattern database;

    @Option( names = "--fallback-to-full", paramLabel = "<true/false>", defaultValue = "true", showDefaultValue = ALWAYS,
            description = "If an incremental backup fails backup will move the old backup to <name>.err.<N> and fallback to a full." )
    private boolean fallbackToFull;

    @Option( names = "--pagecache", paramLabel = "<size>", defaultValue = "8m", description = "The size of the page cache to use for the backup process." )
    private String pagecacheMemory;

    @Option( names = "--check-consistency", paramLabel = "<true/false>", defaultValue = "true", showDefaultValue = ALWAYS,
            description = "If a consistency check should be made." )
    private boolean checkConsistency;

    @Mixin
    private ConsistencyCheckOptions consistencyCheckOptions;

    @Option( names = "--additional-config", paramLabel = "<path>", description = "Configuration file to supply additional configuration in." )
    private Path additionalConfig;

    @Option( names = "--include-metadata", paramLabel = "<all/users/roles>",
            description = "Include metadata in file. Can't be used for backing system database.\n" +
                          "roles - commands to create the roles and privileges (for both database and graph) that affect the use of the database\n" +
                          "users - commands to create the users that can use the database and their role assignments \n" +
                          "all - include roles and users"
    )
    private IncludeMetadata includeMetadata;

    public OnlineBackupCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    protected void execute()
    {
        backupDir = requireExisting( backupDir );
        final var address = SettingValueParsers.SOCKET_ADDRESS.parse( from );
        final var configFile = ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME );
        final var config = buildConfig( configFile, additionalConfig, backupDir );
        final var contextBuilder = OnlineBackupContext.builder()
                                                      .withBackupDirectory( requireExisting( backupDir ) )
                                                      .withReportsDirectory( consistencyCheckOptions.getReportDir() )
                                                      .withAddress( address )
                                                      .withDatabaseNamePattern( database )
                                                      .withConfig( config )
                                                      .withIncludeMetadata( Optional.ofNullable( includeMetadata ) )
                                                      .withFallbackToFullBackup( fallbackToFull )
                                                      .withConsistencyCheck( checkConsistency )
                                                      .withConsistencyCheckGraph( consistencyCheckOptions.isCheckGraph() )
                                                      .withConsistencyCheckIndexes( consistencyCheckOptions.isCheckIndexes() )
                                                      .withConsistencyCheckIndexStructure( consistencyCheckOptions.isCheckIndexStructure() )
                                                      .withConsistencyCheckPropertyOwners( consistencyCheckOptions.isCheckPropertyOwners() )
                                                      .withConsistencyCheckLabelScanStore( consistencyCheckOptions.isCheckLabelScanStore() )
                                                      .withConsistencyCheckRelationshipTypeScanStore(
                                                              consistencyCheckOptions.isCheckRelationshipTypeScanStore() );

        final var userLogProvider = Util.configuredLogProvider( config, ctx.out() );
        final var internalLogProvider = verbose ? userLogProvider : NullLogProvider.getInstance();
        final var backupExecutor = OnlineBackupExecutor.builder()
                                                       .withFileSystem( ctx.fs() )
                                                       .withInternalLogProvider( internalLogProvider )
                                                       .withUserLogProvider( userLogProvider )
                                                       .withClock( Clocks.nanoClock() )
                                                       .withProgressMonitorFactory( ProgressMonitorFactory.textual( ctx.err() ) )
                                                       .build();

        try
        {
            backupExecutor.executeBackups( contextBuilder );
        }
        catch ( ConsistencyCheckExecutionException e )
        {
            throw new CommandFailedException( e.getMessage(), e, e.getExitCode() );
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( "Execution of backup failed. " + getRootCause( e ).getMessage(), e );
        }
        finally
        {
            userLogProvider.close();
        }

        ctx.out().println( "Backup complete successful." );
    }

    private static Path requireExisting( Path p )
    {
        try
        {
            return p.toRealPath();
        }
        catch ( IOException e )
        {
            throw new CommandFailedException( format( "Path '%s' does not exist.", p ), e );
        }
    }

    private Config buildConfig( Path configFile, Path additionalConfigFile, Path backupDirectory )
    {
        Config cfg = Config.newBuilder()
                .fromFileNoThrow( configFile )
                .fromFileNoThrow( additionalConfigFile )
                .set( GraphDatabaseSettings.neo4j_home, backupDirectory )
                .set( pagecache_memory, pagecacheMemory )
                .set( pagecache_warmup_enabled, FALSE )
                .set( OnlineBackupSettings.online_backup_enabled, FALSE )
                .build();
        ConfigUtils.disableAllConnectors( cfg );

        if ( verbose )
        {
            cfg.set( GraphDatabaseSettings.store_internal_log_level, Level.DEBUG );
        }

        return cfg;
    }

}
