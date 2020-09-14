/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            description = "Name of the database after restore. Use of this option is only allowed if a single is provided to the --from option",
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
            description = "Base directory for databases. Use of this option is only allowed if a single is provided to the --from option",
            paramLabel = "<path>"
    )
    private Path databaseRootDirectory;
    @Option(
            names = "--to-data-tx-directory",
            description = "Base directory for transaction logs. Use of this option is only allowed if a single is provided to the --from option",
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
                                  .fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile() )
                                  .set( GraphDatabaseSettings.neo4j_home, homeDir );
        Optional.ofNullable( databaseRootDirectory ).ifPresent( v -> builder.set( GraphDatabaseInternalSettings.databases_root_path, v ) );
        Optional.ofNullable( txRootDirectory ).ifPresent( v -> builder.set( GraphDatabaseSettings.transaction_logs_root_path, v ) );

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
        final var exceptions = execute( paths, neo4jLayout );

        exceptions.collect( Collectors.toList() )
                  .stream()
                  .findFirst()
                  .ifPresent( exception ->
                              {
                                  throw exception;
                              } );
    }

    private Stream<CommandFailedException> execute( Set<Path> filteredPaths, Neo4jLayout neo4jLayout )
    {

        return filteredPaths.stream().map( fromPath ->
                                           {
                                               try
                                               {
                                                   var databaseName = new NormalizedDatabaseName( getDatabaseName( fromPath, database ) ).name();
                                                   final var databaseLayout = neo4jLayout.databaseLayout( databaseName );
                                                   RestoreDatabaseCommand restoreDatabaseCommand =
                                                           new RestoreDatabaseCommand( ctx.fs(), fromPath, databaseLayout, force, move );
                                                   restoreDatabaseCommand.execute();

                                                   ctx.out().printf( "Database with path %s was restored", fromPath );
                                                   return null;
                                               }
                                               catch ( Exception e )
                                               {
                                                   ctx.err().printf( "Database with path %s wasn't restored", fromPath );
                                                   return new CommandFailedException( "Error in executing restore for path " + fromPath, e );
                                               }
                                           } ).filter( Objects::nonNull );
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
}
