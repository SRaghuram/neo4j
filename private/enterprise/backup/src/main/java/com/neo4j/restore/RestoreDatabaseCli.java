/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.configuration.CausalClusteringSettings;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.FromPaths;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.io.layout.Neo4jLayout;

import static org.neo4j.cli.Converters.FromPathsConverter;
import static picocli.CommandLine.Help.Visibility.NEVER;

@Command(
        name = "restore",
        description = "Restore a backed up database."
)
public class RestoreDatabaseCli extends AbstractCommand
{
    @Option( names = "--from",
            paramLabel = "<path>[,<path>...]",
            required = true,
            arity = "1..*",
            description = "Path or paths from which to restore. Every path can contain asterisks or question marks in the last subpath. " +
                          "Multiple paths may be separated by a comma, but paths themselves must not contain commas.",
            converter = FromPathsConverter.class )
    private FromPaths fromPaths;
    @Option( names = "--database",
            description = "Name of the database after restore. Usage of this option is only allowed if --from parameter point to exact one directory",
            showDefaultValue = NEVER,
            converter = DatabaseNameConverter.class )
    private NormalizedDatabaseName database;
    @Option( names = "--force",
            arity = "0",
            description = "If an existing database should be replaced." )
    private boolean force;
    @Option( names = "--move",
            arity = "0",
            description = "Moves the backup files to the destination, rather than copying." )
    private boolean move;
    @Option(
            names = "--to-data-directory",
            description = "Base directory for databases. Usage of this option is only allowed if --from parameter point to exact one directory",
            paramLabel = "<path>"
    )
    private Path databaseRootDirectory;
    @Option(
            names = "--to-data-tx-directory",
            description = "Base directory for transaction logs. Usage of this option is only allowed if --from parameter point to exact one directory",
            paramLabel = "<path>"
    )
    private Path txRootDirectory;

    public RestoreDatabaseCli( ExecutionContext ctx )
    {
        super( ctx );
    }

    private Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        final var builder = Config.newBuilder()
                                  .fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                                  .set( GraphDatabaseSettings.neo4j_home, homeDir );
        Optional.ofNullable( databaseRootDirectory ).ifPresent( v -> builder.set( GraphDatabaseInternalSettings.databases_root_path, v.toAbsolutePath() ) );
        Optional.ofNullable( txRootDirectory ).ifPresent( v -> builder.set( GraphDatabaseSettings.transaction_logs_root_path, v.toAbsolutePath() ) );

        final var config = builder.build();
        ConfigUtils.disableAllConnectors( config );
        return config;
    }

    @Override
    public void execute()
    {
        validateArguments();

        final var config = loadNeo4jConfig( ctx.homeDir(), ctx.confDir() );

        final var paths = fromPaths.getPaths();
        final var neo4jLayout = Neo4jLayout.of( config );
        final var clusterStateLayout = ClusterStateLayout.of( config.get( CausalClusteringSettings.cluster_state_directory ) );
        final var restoreResults = execute( paths, neo4jLayout, clusterStateLayout );
        restoreResults.forEach( this::printRestoreResult );

        final var hasFailedRestore = restoreResults.stream().anyMatch( r -> r.exception.isPresent() );
        if ( hasFailedRestore )
        {
            throw new CommandFailedException( "Restore command wasn't execute successfully" );
        }
    }

    private List<RestoreResult> execute( Set<Path> filteredPaths, Neo4jLayout neo4jLayout, ClusterStateLayout clusterStateLayout )
    {

        return filteredPaths.stream().map( fromPath ->
                                           {
                                               try
                                               {
                                                   final var databaseName = new NormalizedDatabaseName( getDatabaseName( fromPath, database ) ).name();
                                                   final var databaseLayout = neo4jLayout.databaseLayout( databaseName );
                                                   final var raftGroupDirectory = clusterStateLayout.raftGroupDir( databaseName );
                                                   RestoreDatabaseCommand restoreDatabaseCommand =
                                                           new RestoreDatabaseCommand( ctx.fs(), ctx.out(), fromPath, databaseLayout, raftGroupDirectory, force,
                                                                                       move );
                                                   restoreDatabaseCommand.execute();

                                                   return new RestoreResult( fromPath );
                                               }
                                               catch ( Exception e )
                                               {
                                                   return new RestoreResult( fromPath, Optional.of( e ) );
                                               }
                                           } ).collect( Collectors.toList() );
    }

    private String getDatabaseName( Path fromPath, NormalizedDatabaseName database )
    {
        if ( database != null )
        {
            return database.name();
        }
        else
        {
            final String name = fromPath.getName( fromPath.getNameCount() - 1 ).toString();
            if ( !name.trim().isEmpty() )
            {
                return name;
            }
            else
            {
                throw new IllegalArgumentException( "Last subpath of " + fromPath + " is empty" );
            }
        }
    }

    private void validateArguments()
    {
        if ( database != null && !fromPaths.isSingle() )
        {
            throw new CommandFailedException( "--database parameter can be applied only when --from match single path" );
        }
        if ( databaseRootDirectory != null && !fromPaths.isSingle() )
        {
            throw new CommandFailedException( "--to-data-directory parameter can be applied only when --from match single path" );
        }
        if ( txRootDirectory != null && !fromPaths.isSingle() )
        {
            throw new CommandFailedException( "--to-data-tx-directory parameter can be applied only when --from match single path" );
        }
    }

    private void printRestoreResult( RestoreResult restoreResults )
    {
        final var status = restoreResults.exception.isPresent() ? "failed" : "successful";
        final var reason = restoreResults.exception.map( Throwable::getMessage ).orElse( "" );

        ctx.out()
           .println( String.format( "restorePath=%s, restoreStatus=%s, reason=%s", restoreResults.fromPath.toAbsolutePath().toString(), status, reason ) );
    }

    private static final class RestoreResult
    {
        private final Optional<Exception> exception;
        private final Path fromPath;

        private RestoreResult( Path fromPath )
        {
            this( fromPath, Optional.empty() );
        }

        private RestoreResult( Path fromPath, Optional<Exception> exception )
        {
            this.fromPath = fromPath;
            this.exception = exception;
        }
    }
}
