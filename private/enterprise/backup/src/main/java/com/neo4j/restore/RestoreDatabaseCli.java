/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.PlaceholderDatabaseIdRepository;

@Command(
        name = "restore",
        description = "Restore a backed up database."
)
class RestoreDatabaseCli extends AbstractCommand
{
    @Option( names = "--from", paramLabel = "<path>", required = true, description = "Path to backup to restore from." )
    private File from;
    @Option( names = "--database", description = "Name of database.", defaultValue = GraphDatabaseSettings.DEFAULT_DATABASE_NAME )
    private String database;
    @Option( names = "--force", arity = "0", description = "If an existing database should be replaced." )
    private boolean force;

    RestoreDatabaseCli( ExecutionContext ctx )
    {
        super( ctx );
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        Config cfg = Config.newBuilder()
                .fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile() )
                .set( GraphDatabaseSettings.neo4j_home, homeDir.toString() ).build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
    }

    @Override
    protected void execute() throws IOException
    {
        Config config = loadNeo4jConfig( ctx.homeDir(), ctx.confDir() );
        DatabaseIdRepository databaseIdRepository = new PlaceholderDatabaseIdRepository( config );
        DatabaseId databaseId = databaseIdRepository.get( database );

        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( ctx.fs(), from, config, databaseId, force );
        restoreDatabaseCommand.execute();
    }
}
