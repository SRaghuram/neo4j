/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.Settings;
import org.neo4j.consistency.ConsistencyCheckOptions;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.DEFAULT_BACKUP_HOST;
import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.DEFAULT_BACKUP_PORT;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_enabled;
import static org.neo4j.kernel.impl.util.Converters.toAdvertisedSocketAddress;
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
    private static final int STATUS_CONSISTENCY_CHECK_ERROR = 2;
    private static final int STATUS_CONSISTENCY_CHECK_INCONSISTENT = 3;

    @Option( names = "--backup-dir", paramLabel = "<path>", required = true, description = "Directory to place backup in." )
    private Path backupDir;

    @Option( names = "--from", paramLabel = "<host:port>", defaultValue = DEFAULT_BACKUP_HOST + ":" + DEFAULT_BACKUP_PORT,
            description = "Host and port of Neo4j." )
    private String from;

    @Option( names = "--database", defaultValue = DEFAULT_DATABASE_NAME, description = "Name of the remote database to backup." )
    private String database;

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

    public OnlineBackupCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    protected void execute()
    {
        backupDir = requireExisting( backupDir );
        final var address = getAddress( from );
        final var configFile = ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME );
        final var config = buildConfig( configFile, additionalConfig, backupDir );
        final var onlineBackupContext = OnlineBackupContext.builder()
                .withBackupDirectory( requireExisting( backupDir ) )
                .withReportsDirectory( consistencyCheckOptions.getReportDir() )
                .withAddress( address )
                .withDatabaseName( database )
                .withConfig( config )
                .withFallbackToFullBackup( fallbackToFull )
                .withConsistencyCheck( checkConsistency )
                .withConsistencyCheckGraph( consistencyCheckOptions.isCheckGraph() )
                .withConsistencyCheckIndexes( consistencyCheckOptions.isCheckIndexes() )
                .withConsistencyCheckPropertyOwners( consistencyCheckOptions.isCheckPropertyOwners() )
                .withConsistencyCheckLabelScanStore( consistencyCheckOptions.isCheckLabelScanStore() )
                .build();

        final var userLogProvider = FormattedLogProvider.toOutputStream( ctx.out() );
        final var internalLogProvider = buildInternalLogProvider();
        final var backupExecutor = OnlineBackupExecutor.builder()
                .withFileSystem( ctx.fs() )
                .withInternalLogProvider( internalLogProvider )
                .withUserLogProvider( userLogProvider )
                .withProgressMonitorFactory( ProgressMonitorFactory.textual( ctx.err() ) )
                .build();

        try
        {
            backupExecutor.executeBackup( onlineBackupContext );
        }
        catch ( ConsistencyCheckExecutionException e )
        {
            int exitCode = e.consistencyCheckFailedToExecute() ? STATUS_CONSISTENCY_CHECK_ERROR : STATUS_CONSISTENCY_CHECK_INCONSISTENT;
            throw new CommandFailedException( e.getMessage(), e, exitCode );
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( "Execution of backup failed", e );
        }

        ctx.out().println( "Backup complete." );
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

    private static AdvertisedSocketAddress getAddress( String s )
    {
        return toAdvertisedSocketAddress( new HostnamePort( s ), DEFAULT_BACKUP_HOST, DEFAULT_BACKUP_PORT );
    }

    private Config buildConfig( Path configFile, Path additionalConfigFile, Path backupDirectory )
    {
        Config config = Config.fromFile( configFile )
                .withHome( backupDirectory )
                .withConnectorsDisabled()
                .withNoThrowOnFileLoadFailure() // Online backup does not require the presence of a neo4j.conf file.
                .build();

        if ( additionalConfigFile != null )
        {
            Config additionalConfig = Config.fromFile( additionalConfigFile ).build();
            config.augment( additionalConfig );
        }

        // We replace the page cache memory setting.
        // Any other custom page swapper, etc. settings are preserved and used.
        config.augment( pagecache_memory, pagecacheMemory );
        // warmup is also disabled because it is not needed for temporary databases
        config.augment( pagecache_warmup_enabled.name(), Settings.FALSE );

        // Disable all metrics to avoid port binding and JMX naming exceptions
        config.augment( "metrics.enabled", Settings.FALSE );
        return config;
    }

    private LogProvider buildInternalLogProvider()
    {
        if ( verbose )
        {
            return FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( ctx.out() );
        }
        return NullLogProvider.getInstance();
    }
}
