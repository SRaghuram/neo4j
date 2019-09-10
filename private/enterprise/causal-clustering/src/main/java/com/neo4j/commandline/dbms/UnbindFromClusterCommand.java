/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.DatabaseLockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(
        name = "unbind",
        header = "Removes cluster state data for the specified database.",
        description = "Removes cluster state data for the specified database, so that the instance can rebind to a new or recovered cluster."
)
class UnbindFromClusterCommand extends AbstractCommand
{
    @Option( names = "--database", description = "Name of the database.", defaultValue = DEFAULT_DATABASE_NAME )
    private String database;

    UnbindFromClusterCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        return Config.newBuilder()
                .fromFileNoThrow( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .set( GraphDatabaseSettings.neo4j_home, homeDir )
                .build();
    }

    @Override
    public void execute()
    {
        try
        {
            Config config = loadNeo4jConfig( ctx.homeDir(), ctx.confDir() );
            File dataDirectory = config.get( GraphDatabaseSettings.data_directory ).toFile();
            File databasesRoot = config.get( databases_root_path ).toFile();
            File homeDir = config.get( neo4j_home ).toFile();
            DatabaseLayout databaseLayout = DatabaseLayout.of( homeDir, databasesRoot, database );

            try ( Closeable ignored = validateDatabase( databaseLayout ) )
            {
                File clusterStateDirectory = ClusterStateLayout.of( dataDirectory ).getClusterStateDirectory();

                if ( ctx.fs().fileExists( clusterStateDirectory ) )
                {
                    deleteClusterStateIn( clusterStateDirectory );
                }
                else
                {
                    ctx.err().println( "This instance was not bound. No work performed." );
                }
            }
        }
        catch ( FileLockException e )
        {
            throw new CommandFailedException( "Database is currently locked. Please shutdown database.", e );
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( e.getMessage(), e );
        }
    }

    private Closeable validateDatabase( DatabaseLayout databaseLayout ) throws CannotWriteException
    {
        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( databaseLayout.databaseDirectory() );
            return DatabaseLockChecker.check( databaseLayout );
        }
        catch ( IllegalArgumentException ignored )
        {
            // No such database, it must have been deleted. Must be OK to delete cluster state
            return () -> { };
        }
    }

    private void deleteClusterStateIn( File target ) throws UnbindFailureException
    {
        try
        {
            ctx.fs().deleteRecursively( target );
        }
        catch ( IOException e )
        {
            throw new UnbindFailureException( e );
        }
    }

    private static class UnbindFailureException extends Exception
    {
        UnbindFailureException( Exception e )
        {
            super( e );
        }
    }
}
