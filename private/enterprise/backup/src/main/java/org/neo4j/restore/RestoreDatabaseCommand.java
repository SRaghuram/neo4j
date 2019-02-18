/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.restore;

import java.io.File;
import java.io.IOException;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.Validators;

import static java.lang.String.format;
import static org.neo4j.commandline.Util.checkLock;
import static org.neo4j.commandline.Util.isSameOrChildFile;
import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;

public class RestoreDatabaseCommand
{
    private FileSystemAbstraction fs;
    private final File fromDatabasePath;
    private final DatabaseLayout targetDatabaseLayout;
    private final String toDatabaseName;
    private boolean forceOverwrite;

    public RestoreDatabaseCommand( FileSystemAbstraction fs, File fromDatabasePath, Config config, String toDatabaseName,
            boolean forceOverwrite )
    {
        this.fs = fs;
        this.fromDatabasePath = fromDatabasePath;
        this.toDatabaseName = toDatabaseName;
        this.forceOverwrite = forceOverwrite;
        this.targetDatabaseLayout = DatabaseLayout.of( config.get( database_path ).getAbsoluteFile(), of( config ) );
    }

    public void execute() throws IOException, CommandFailed
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
            throw new IllegalArgumentException(
                    format( "Database with name [%s] already exists at %s", toDatabaseName, targetDatabaseLayout ) );
        }

        checkLock( targetDatabaseLayout.getStoreLayout() );

        fs.deleteRecursively( targetDatabaseLayout.databaseDirectory() );

        if ( !isSameOrChildFile( targetDatabaseLayout.databaseDirectory(), targetDatabaseLayout.getTransactionLogsDirectory() ) )
        {
            fs.deleteRecursively( targetDatabaseLayout.getTransactionLogsDirectory() );
        }
        LogFiles backupLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( fromDatabasePath, fs ).build();
        restoreDatabaseFiles( backupLogFiles, fromDatabasePath.listFiles() );
    }

    private void restoreDatabaseFiles( LogFiles backupLogFiles, File[] files ) throws IOException
    {
        if ( files != null )
        {
            File databaseDirectory = targetDatabaseLayout.databaseDirectory();
            File transactionLogsDirectory = targetDatabaseLayout.getTransactionLogsDirectory();
            for ( File file : files )
            {
                if ( file.isDirectory() )
                {
                    File destination = new File( databaseDirectory, file.getName() );
                    fs.mkdirs( destination );
                    fs.copyRecursively( file, destination );
                }
                else
                {
                    fs.copyToDirectory( file, backupLogFiles.isLogFile( file ) ? transactionLogsDirectory : databaseDirectory );
                }
            }
        }
    }
}
