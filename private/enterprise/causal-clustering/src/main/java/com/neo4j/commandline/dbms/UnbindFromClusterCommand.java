/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.locker.FileLockException;

import static picocli.CommandLine.Command;

@Command(
        name = "unbind",
        header = "Removes all cluster state.",
        description = "Removes all cluster state, so that the instance can rebind to a cluster."
)
public class UnbindFromClusterCommand extends AbstractCommand
{
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
            Neo4jLayout neo4jLayout = Neo4jLayout.of( config );

            try ( Closeable ignored = LockChecker.checkDbmsLock( neo4jLayout ) )
            {
                Path clusterStateDirectory = ClusterStateLayout.of( config.get( GraphDatabaseSettings.data_directory ) ).getClusterStateDirectory();

                if ( ctx.fs().fileExists( clusterStateDirectory.toFile() ) )
                {
                    deleteClusterStateIn( clusterStateDirectory.toFile() );
                }
                else
                {
                    ctx.err().println( "This instance was not bound. No work performed." );
                }

                ctx.fs().deleteFile( neo4jLayout.serverIdFile().toFile() );
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
