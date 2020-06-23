/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;

import static java.lang.String.format;
import static org.neo4j.commandline.Util.isSameOrChildFile;

public class RestoreDatabaseCommand
{
    private final FileSystemAbstraction fs;
    private final Path fromDatabasePath;
    private final DatabaseLayout targetDatabaseLayout;
    private final File raftGroupDirectory;
    private final boolean forceOverwrite;
    private final boolean moveFiles;

    public RestoreDatabaseCommand( FileSystemAbstraction fs, Path fromDatabasePath, Config config, String databaseName, boolean forceOverwrite,
                                   boolean moveFiles )
    {
        this.fs = fs;
        this.fromDatabasePath = fromDatabasePath;
        this.forceOverwrite = forceOverwrite;
        this.moveFiles = moveFiles;
        this.targetDatabaseLayout = buildTargetDatabaseLayout( databaseName, config );
        this.raftGroupDirectory = getRaftGroupDirectory( databaseName, config );
    }

    public void execute() throws IOException
    {
        if ( !fs.fileExists( fromDatabasePath.toFile() ) )
        {
            throw new IllegalArgumentException( format( "Source directory does not exist [%s]", fromDatabasePath ) );
        }

        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( fromDatabasePath.toFile() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalArgumentException(
                    format( "Source directory is not a database backup [%s]", fromDatabasePath ), e );
        }

        if ( fs.fileExists( targetDatabaseLayout.databaseDirectory().toFile() ) && !forceOverwrite )
        {
            throw new IllegalArgumentException( format( "Database with name [%s] already exists at %s", targetDatabaseLayout.getDatabaseName(),
                    targetDatabaseLayout.databaseDirectory().toFile() ) );
        }

        if ( fs.fileExists( raftGroupDirectory ) )
        {
            throw new IllegalArgumentException( format(
                    "Database with name [%s] already exists locally. " +
                    "Please run `DROP DATABASE %s` against the system database. " +
                    "If the database already is dropped, then you need to unbind the local instance using `neo4j-admin unbind`. " +
                    "Note that unbind requires stopping the instance, and affects all databases.",
                    targetDatabaseLayout.getDatabaseName(), targetDatabaseLayout.getDatabaseName() ) );
        }

        fs.mkdirs( targetDatabaseLayout.databaseDirectory().toFile() );

        try ( var ignored = LockChecker.checkDatabaseLock( targetDatabaseLayout ) )
        {
            cleanTargetDirectories();
            restoreDatabaseFiles();
        }
        catch ( FileLockException e )
        {
            throw new CommandFailedException( "The database is in use. Stop database '" + targetDatabaseLayout.getDatabaseName() + "' and try again.", e );
        }
        catch ( CannotWriteException e )
        {
            throw new CommandFailedException( "You do not have permission to restore database.", e );
        }
    }

    private void cleanTargetDirectories() throws IOException
    {
        var databaseDirectory = targetDatabaseLayout.databaseDirectory().toFile();
        var transactionLogsDirectory = targetDatabaseLayout.getTransactionLogsDirectory().toFile();
        var databaseLockFile = targetDatabaseLayout.databaseLockFile().toFile();

        var filesToRemove = fs.listFiles( databaseDirectory, ( dir, name ) -> !name.equals( databaseLockFile.getName() ) );
        if ( filesToRemove != null )
        {
            for ( var file : filesToRemove )
            {
                fs.deleteRecursively( file );
            }
        }
        if ( !isSameOrChildFile( databaseDirectory, transactionLogsDirectory ) )
        {
            fs.deleteRecursively( transactionLogsDirectory );
        }
    }

    private void restoreDatabaseFiles() throws IOException
    {
        var databaseFiles = fs.listFiles( fromDatabasePath.toFile() );
        var transactionLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( fromDatabasePath.toFile(), fs ).build();

        if ( databaseFiles != null )
        {
            var databaseDirectory = targetDatabaseLayout.databaseDirectory().toFile();
            var transactionLogsDirectory = targetDatabaseLayout.getTransactionLogsDirectory().toFile();
            var databaseLockFile = targetDatabaseLayout.databaseLockFile().toFile();
            for ( var file : databaseFiles )
            {
                if ( file.isDirectory() )
                {
                    if ( moveFiles )
                    {
                        fs.moveToDirectory( file, databaseDirectory );
                    }
                    else
                    {
                        var destination = new File( databaseDirectory, file.getName() );
                        fs.mkdirs( destination );
                        fs.copyRecursively( file, destination );
                    }
                }
                else
                {
                    var targetDirectory = transactionLogFiles.isLogFile( file ) ? transactionLogsDirectory : databaseDirectory;
                    var targetFile = new File( targetDirectory, file.getName() );
                    if ( !databaseLockFile.equals( targetFile ) )
                    {
                        if ( moveFiles )
                        {
                            fs.moveToDirectory( file, targetDirectory );
                        }
                        else
                        {
                            fs.copyToDirectory( file, targetDirectory );
                        }
                    }
                }
            }
            if ( moveFiles )
            {
                fs.deleteRecursively( fromDatabasePath.toFile() );
            }
        }
    }

    private static DatabaseLayout buildTargetDatabaseLayout( String databaseName, Config config )
    {
        return Neo4jLayout.of( config ).databaseLayout( databaseName );
    }

    private File getRaftGroupDirectory( String databaseName, Config config )
    {
        return ClusterStateLayout.of( config.get( GraphDatabaseSettings.data_directory ).toFile() ).raftGroupDir( databaseName );
    }
}
