/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.configuration.CausalClusteringSettings;
import picocli.CommandLine;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.Utils;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.time.Clocks;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(
        name = "unbind",
        header = "Removes and archives all cluster state.",
        description = "Removes and archives all cluster state, so that the instance can rebind to a cluster."
)
public class UnbindFromClusterCommand extends AbstractCommand
{
    @CommandLine.Option( names = "--archive-cluster-state", arity = "1", required = false, showDefaultValue = ALWAYS, paramLabel = "<true/false>",
            description = "Enable or disable the cluster state archiving." )
    private boolean isClusterStateArchiveEnabled;

    @CommandLine.Option( names = "--archive-path", required = false, paramLabel = "<path>",
            description = "Destination (file or folder) of the cluster state archive." )
    private Path clusterStateArchiveFile;

    private Clock clock;

    private static String ARCHIVE_DEFAULT_NAME_PATTERN = "unbound_cluster_state.%s.zip";

    private static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                                                             .withZone( ZoneId.systemDefault());

    public UnbindFromClusterCommand( ExecutionContext ctx )
    {
        super( ctx );
        this.clock = Clocks.systemClock();
    }

    public UnbindFromClusterCommand( ExecutionContext ctx, Clock clock )
    {
        super( ctx );
        this.clock = clock;
    }

    private Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        return Config.newBuilder()
                     .fromFileNoThrow( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                     .set( GraphDatabaseSettings.neo4j_home, homeDir )
                     .commandExpansion( allowCommandExpansion )
                     .build();
    }

    @Override
    public void execute()
    {
        try
        {
            Config config = loadNeo4jConfig( ctx.homeDir(), ctx.confDir() );
            Neo4jLayout neo4jLayout = Neo4jLayout.of( config );

            try ( Closeable ignored = LockChecker.checkDbmsLock( neo4jLayout ) )
            {
                Path clusterStateDirectory = ClusterStateLayout.of( config.get( CausalClusteringSettings.cluster_state_directory ) ).getClusterStateDirectory();

                if ( ctx.fs().fileExists( clusterStateDirectory ) )
                {
                    if ( isClusterStateArchiveEnabled )
                    {
                        Path archive = buildArchivePath( neo4jLayout, clusterStateArchiveFile );
                        archiveClusterState( archive, clusterStateDirectory, neo4jLayout.serverIdFile() );
                    }
                    deleteClusterStateIn( clusterStateDirectory );
                }
                else
                {
                    ctx.err().println( "This instance was not bound. No work performed." );
                }

                ctx.fs().deleteFile( neo4jLayout.serverIdFile() );
            }
        }
        catch ( FileLockException e )
        {
            throw new CommandFailedException( "Database is currently locked. Please shutdown database.", e );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getMessage(), e );
        }
    }

    private void archiveClusterState( Path archive, Path clusterStateDirectory, Path serverIdfile ) throws IOException
    {
        checkWritableFile( archive );

        zip( clusterStateDirectory, archive );

        if ( Files.exists( serverIdfile ) )
        {
            zip( serverIdfile, archive );
        }
    }

    private void zip( Path source, Path target ) throws IOException
    {
        if ( Files.isReadable( source ) )
        {
            try
            {
                ZipUtils.zip( ctx.fs(), source, target );
            }
            catch ( IOException e )
            {
                Files.deleteIfExists( target );
                throw new IOException(
                        format( "Unable to create the archive: %s: %s", target.toString(), e.getMessage() ), e );
            }
        }
        else
        {
            Files.deleteIfExists( target );
            throw new IOException( format( "Can not create the archive %s because the following source is not readable: %s.",
                                           target.toString(), source.toString() ) );
        }
    }

    private Path buildArchivePath( Neo4jLayout neo4jLayout, Path clusterStateArchiveFile )
    {
        if ( Objects.isNull( clusterStateArchiveFile ) )
        {
            return neo4jLayout.homeDirectory().resolve( generateDefaultArchiveName() );
        }
        else
        {
            return Files.isDirectory( clusterStateArchiveFile )
                   ? clusterStateArchiveFile.resolve( generateDefaultArchiveName() )
                   : clusterStateArchiveFile;
        }
    }

    private String generateDefaultArchiveName()
    {
        String formattedInstant = DATE_TIME_FORMATTER.format( clock.instant() );
        return format( ARCHIVE_DEFAULT_NAME_PATTERN, formattedInstant );
    }

    private void deleteClusterStateIn( Path target ) throws IOException
    {
        try
        {
            ctx.fs().deleteRecursively( target );
        }
        catch ( IOException e )
        {
            throw new IOException(
                    format( "Unable to delete the cluster state directory: %s: %s", target.toString(), e.getMessage() ), e );
        }
    }

    private void checkWritableFile( Path filePath ) throws FileSystemException
    {
        try
        {
            Utils.checkWritableDirectory( filePath.getParent() );
        }
        catch ( FileSystemException e )
        {
           throw new FileSystemException( filePath.getParent().toAbsolutePath().toString(),
                                          null,
                                          format( "Can't write the archive because the destination directory is not writable." ) );
        }

        if ( Files.exists( filePath ) )
        {
            throw new FileAlreadyExistsException( filePath.toAbsolutePath().toString(), null, format( "Archive already exists.") );
        }
    }
}
