/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Command(
        name = "restore",
        description = "Restore a backed up database."
)
public class RestoreDatabaseCli extends AbstractCommand
{
    @Option( names = "--from", paramLabel = "<path>", required = true, description = "Path to backup to restore from." )
    private Path from;
    @Option( names = "--database", description = "Name of the database to restore.", defaultValue = DEFAULT_DATABASE_NAME,
            converter = DatabaseNameConverter.class )
    private NormalizedDatabaseName database;
    @Option( names = "--force", arity = "0", description = "If an existing database should be replaced." )
    private boolean force;
    @Option( names = "--move", arity = "0", description = "Moves the backup files to the destination, rather than copying." )
    private boolean move;

    public RestoreDatabaseCli( ExecutionContext ctx )
    {
        super( ctx );
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        Config cfg = Config.newBuilder()
                .fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile() )
                .set( GraphDatabaseSettings.neo4j_home, homeDir ).build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
    }

    @Override
    public void execute() throws IOException
    {
        Config config = loadNeo4jConfig( ctx.homeDir(), ctx.confDir() );

        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( ctx.fs(), from, config, database.name(), force, move );
        restoreDatabaseCommand.execute();
    }
}
