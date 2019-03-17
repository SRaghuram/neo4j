/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.commandline.dbms;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.impl.util.Validators;

import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;
import static org.neo4j.configuration.Config.fromFile;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;

public class UnbindFromClusterCommand implements AdminCommand
{
    private static final Arguments arguments = new Arguments().withDatabase();
    private Path homeDir;
    private Path configDir;
    private OutsideWorld outsideWorld;

    UnbindFromClusterCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    static Arguments arguments()
    {
        return arguments;
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        return fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withNoThrowOnFileLoadFailure()
                .withHome( homeDir ).build();
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        try
        {
            String databaseName = arguments.parse( args ).get( ARG_DATABASE );
            Config config = loadNeo4jConfig( homeDir, configDir );
            File dataDirectory = config.get( GraphDatabaseSettings.data_directory );
            File databasesRoot = config.get( databases_root_path );
            DatabaseLayout databaseLayout = DatabaseLayout.of( databasesRoot, databaseName );

            boolean hasDatabase = true;
            try
            {
                Validators.CONTAINS_EXISTING_DATABASE.validate( databaseLayout.databaseDirectory() );
            }
            catch ( IllegalArgumentException ignored )
            {
                // No such database, it must have been deleted. Must be OK to delete cluster state
                hasDatabase = false;
            }

            if ( hasDatabase )
            {
                confirmTargetDirectoryIsWritable( databaseLayout.getStoreLayout() );
            }

            File clusterStateDirectory = ClusterStateLayout.of( dataDirectory ).getClusterStateDirectory();

            if ( outsideWorld.fileSystem().fileExists( clusterStateDirectory ) )
            {
                deleteClusterStateIn( clusterStateDirectory );
            }
            else
            {
                outsideWorld.stdErrLine( "This instance was not bound. No work performed." );
            }
        }
        catch ( StoreLockException e )
        {
            throw new CommandFailed( "Database is currently locked. Please shutdown Neo4j.", e );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        catch ( Exception e )
        {
            throw new CommandFailed( e.getMessage(), e );
        }
    }

    private static void confirmTargetDirectoryIsWritable( StoreLayout storeLayout )
            throws CannotWriteException, IOException
    {
        try ( Closeable ignored = StoreLockChecker.check( storeLayout ) )
        {
            // empty
        }
    }

    private void deleteClusterStateIn( File target ) throws UnbindFailureException
    {
        try
        {
            outsideWorld.fileSystem().deleteRecursively( target );
        }
        catch ( IOException e )
        {
            throw new UnbindFailureException( e );
        }
    }

    private class UnbindFailureException extends Exception
    {
        UnbindFailureException( Exception e )
        {
            super( e );
        }
    }
}
