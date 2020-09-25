/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import com.neo4j.backup.impl.DatabaseIdStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
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
    private final Path raftGroupDirectory;
    private final boolean forceOverwrite;
    private final boolean moveFiles;

    public RestoreDatabaseCommand( FileSystemAbstraction fs, Path fromDatabasePath, DatabaseLayout targetDatabaseLayout, Path raftGroupDirectory,
                                   boolean forceOverwrite, boolean moveFiles )
    {
        this.fs = fs;
        this.fromDatabasePath = fromDatabasePath;
        this.forceOverwrite = forceOverwrite;
        this.moveFiles = moveFiles;
        this.targetDatabaseLayout = targetDatabaseLayout;
        this.raftGroupDirectory = raftGroupDirectory;
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
                    format( "Source directory is not a database backup [%s]", fromDatabasePath ), e );
        }

        if ( fs.fileExists( targetDatabaseLayout.databaseDirectory() ) && !forceOverwrite )
        {
            throw new IllegalArgumentException( format( "Database with name [%s] already exists at %s", targetDatabaseLayout.getDatabaseName(),
                    targetDatabaseLayout.databaseDirectory() ) );
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

        var filesToRemove = fs.listFiles( databaseDirectory, path -> !path.getFileName().equals( databaseLockFile.getFileName() ) );
        if ( filesToRemove != null )
        {
            for ( var file : filesToRemove )
            {
                fs.delete( file );
            }
        }
        if ( !isSameOrChildFile( databaseDirectory, transactionLogsDirectory ) )
        {
            fs.deleteRecursively( transactionLogsDirectory );
        }
    }

    private void restoreDatabaseFiles() throws IOException
    {
        var databaseFiles = Optional.ofNullable( fs.listFiles( fromDatabasePath, path -> !path.getFileName().toString().equals( DatabaseIdStore.FILE_NAME ) ) )
                                    .map( files -> Arrays.stream( files )
                                                         .collect( Collectors.toSet() ) )
                                    .orElse( Set.of() );

        var transactionLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( fromDatabasePath, fs ).build();

        var databaseDirectory = targetDatabaseLayout.databaseDirectory();
        var transactionLogsDirectory = targetDatabaseLayout.getTransactionLogsDirectory();
        var databaseLockFile = targetDatabaseLayout.databaseLockFile();
        for ( var file : databaseFiles )
        {
            if ( Files.isDirectory( file ) )
            {
                    if ( moveFiles )
                    {
                        fs.moveToDirectory( file, databaseDirectory );
                    }
                    else
                    {
                        var destination = databaseDirectory.resolve( file.getFileName() );
                        fs.mkdirs( destination );
                        fs.copyRecursively( file, destination );
                    }
                }
                else
                {
                    var targetDirectory = transactionLogFiles.isLogFile( file ) ? transactionLogsDirectory : databaseDirectory;
                    var targetFile = targetDirectory.resolve( file.getFileName() );
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
                fs.deleteRecursively( fromDatabasePath );
            }
    }
}
