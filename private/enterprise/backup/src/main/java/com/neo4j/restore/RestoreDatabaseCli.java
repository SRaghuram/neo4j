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
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.configuration.helpers.FromPaths;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.io.layout.Neo4jLayout;

import static org.neo4j.cli.Converters.DatabaseNamePatternConverter;
import static org.neo4j.cli.Converters.FromPathConverter;
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
            description = "Path from where to do a backup.",
            converter = FromPathConverter.class )
    private FromPaths fromPaths;
    @Option( names = "--database",
            description = "Name of the database to restore. Cannot be combined with --filter",
            showDefaultValue = NEVER,
            converter = DatabaseNameConverter.class )
    private NormalizedDatabaseName database;
    @Option( names = "--filter",
            description = "Filter --from folders based on the passed pattern. Cannot be combined with --database",
            converter = DatabaseNamePatternConverter.class )
    private DatabaseNamePattern pattern;
    @Option( names = "--force",
            arity = "0",
            description = "If an existing database should be replaced." )
    private boolean force;
    @Option( names = "--move",
            arity = "0",
            description = "Moves the backup files to the destination, rather than copying." )
    private boolean move;
    @Option(
            names = "--neo4j-home-directory",
            description = "Home directory of restored database.",
            paramLabel = "<path>"
    )
    private Path toHome;

    public RestoreDatabaseCli( ExecutionContext ctx )
    {
        super( ctx );
    }

    private Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        final var config = Config.newBuilder()
                                 .fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile() )
                                 .set( GraphDatabaseSettings.neo4j_home, homeDir )
                                 .build();

        ConfigUtils.disableAllConnectors( config );
        return config;
    }

    @Override
    public void execute()
    {
        validateArguments();

        final var config = loadNeo4jConfig( ctx.homeDir(), ctx.confDir() );

        final var paths = fromPaths.paths( Optional.ofNullable( pattern ) );
        final var neo4jLayout = toHome != null ? Neo4jLayout.of( toHome ) : Neo4jLayout.of( config );
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
        else if ( fromPath.getNameCount() > 0 )
        {
            final String name = fromPath.getName( fromPath.getNameCount() - 1 ).toString();
            if ( !name.isEmpty() )
            {
                return name;
            }
        }
        throw new IllegalArgumentException( "From path with value=" + fromPath + " should not point to the root of the file system" );
    }

    private void validateArguments()
    {
        if ( pattern != null && database != null )
        {
            throw new CommandFailedException( "Can't define database name and filter parameters together" );
        }
        if ( database != null && !fromPaths.isSingle() )
        {
            throw new CommandFailedException( "Database parameter can be applied only when --fromPaths is single value" );
        }
    }
}
