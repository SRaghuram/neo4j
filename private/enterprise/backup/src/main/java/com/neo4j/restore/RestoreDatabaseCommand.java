/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import java.io.File;
import java.io.IOException;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
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
    private final File fromDatabasePath;
    private final DatabaseLayout targetDatabaseLayout;
    private final boolean forceOverwrite;

    public RestoreDatabaseCommand( FileSystemAbstraction fs, File fromDatabasePath, Config config, String databaseName, boolean forceOverwrite )
    {
        this.fs = fs;
        this.fromDatabasePath = fromDatabasePath;
        this.forceOverwrite = forceOverwrite;
        this.targetDatabaseLayout = buildTargetDatabaseLayout( databaseName, config );
    }

    public void execute() throws IOException
    {
        if ( !fs.fileExists( fromDatabasePath ) )
        {
            throw new IllegalArgumentException( format( "Source directory does not exist [%s]", fromDatabasePath ) );
        }

        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( fromDatabasePath );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalArgumentException(
                    format( "Source directory is not a database backup [%s]", fromDatabasePath ) );
        }

        if ( fs.fileExists( targetDatabaseLayout.databaseDirectory() ) && !forceOverwrite )
        {
            throw new IllegalArgumentException( format( "Database with name [%s] already exists at %s", targetDatabaseLayout.getDatabaseName(),
                    targetDatabaseLayout.databaseDirectory() ) );
        }
        fs.mkdirs( targetDatabaseLayout.databaseDirectory() );

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
        var databaseDirectory = targetDatabaseLayout.databaseDirectory();
        var transactionLogsDirectory = targetDatabaseLayout.getTransactionLogsDirectory();
        var databaseLockFile = targetDatabaseLayout.databaseLockFile();

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
        var databaseFiles = fs.listFiles( fromDatabasePath );
        var transactionLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( fromDatabasePath, fs ).build();

        if ( databaseFiles != null )
        {
            var databaseDirectory = targetDatabaseLayout.databaseDirectory();
            var transactionLogsDirectory = targetDatabaseLayout.getTransactionLogsDirectory();
            var databaseLockFile = targetDatabaseLayout.databaseLockFile();
            for ( var file : databaseFiles )
            {
                if ( file.isDirectory() )
                {
                    var destination = new File( databaseDirectory, file.getName() );
                    fs.mkdirs( destination );
                    fs.copyRecursively( file, destination );
                }
                else
                {
                    var targetDirectory = transactionLogFiles.isLogFile( file ) ? transactionLogsDirectory : databaseDirectory;
                    var targetFile = new File( targetDirectory, file.getName() );
                    if ( !databaseLockFile.equals( targetFile ) )
                    {
                        fs.copyToDirectory( file, targetDirectory );
                    }
                }
            }
        }
    }

    private static DatabaseLayout buildTargetDatabaseLayout( String databaseName, Config config )
    {
        var normalizedDatabaseName = new NormalizedDatabaseName( databaseName );
        return Neo4jLayout.of( config ).databaseLayout( normalizedDatabaseName.name() );
    }
}
