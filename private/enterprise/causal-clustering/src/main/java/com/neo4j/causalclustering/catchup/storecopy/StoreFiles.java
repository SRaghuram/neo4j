/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.configuration.CausalClusteringInternalSettings.TEMP_BOOTSTRAP_DIRECTORY_NAME;
import static com.neo4j.configuration.CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME;
import static com.neo4j.configuration.CausalClusteringInternalSettings.TEMP_STORE_COPY_DIRECTORY_NAME;
import static org.neo4j.storageengine.api.StorageEngineFactory.selectStorageEngine;

public class StoreFiles
{
    public static final DirectoryStream.Filter<Path> EXCLUDE_TEMPORARY_DIRS =
            path -> !path.getFileName().toString().equals( TEMP_STORE_COPY_DIRECTORY_NAME ) &&
                    !path.getFileName().toString().equals( TEMP_BOOTSTRAP_DIRECTORY_NAME ) &&
                    !path.getFileName().toString().equals( TEMP_SAVE_DIRECTORY_NAME );

    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final DirectoryStream.Filter<Path> filenameFilter;

    public StoreFiles( FileSystemAbstraction fs, PageCache pageCache )
    {
        this( fs, pageCache, EXCLUDE_TEMPORARY_DIRS );
    }

    public StoreFiles( FileSystemAbstraction fs, PageCache pageCache, DirectoryStream.Filter<Path> filenameFilter )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.filenameFilter = filenameFilter;
    }

    public void delete( DatabaseLayout databaseLayout, LogFiles logFiles ) throws IOException
    {
        Path databaseDirectory = databaseLayout.databaseDirectory();
        Path[] files = fs.listFiles( databaseDirectory, filenameFilter );
        if ( files != null )
        {
            for ( Path file : files )
            {
                fs.delete( file );
            }
        }

        delete( logFiles );
        fs.deleteFile( databaseDirectory );
    }

    public void delete( LogFiles logFiles )
    {
        for ( Path txLog : logFiles.logFiles() )
        {
            fs.deleteFile( txLog );
        }
    }

    public void moveTo( Path source, DatabaseLayout target, LogFiles logFiles ) throws IOException
    {
        fs.mkdirs( logFiles.logFilesDirectory() );

        Path[] files = fs.listFiles( source, filenameFilter );
        if ( files.length != 0 )
        {
            for ( Path file : files )
            {
                Path destination = logFiles.isLogFile( file ) ? target.getTransactionLogsDirectory() : target.databaseDirectory();
                fs.moveToDirectory( file, destination );
            }
        }
    }

    public boolean isEmpty( DatabaseLayout databaseLayout )
    {
        Set<Path> storeFiles = databaseLayout.storeFiles();

        Path[] files = fs.listFiles( databaseLayout.databaseDirectory() );
        if ( files != null )
        {
            for ( Path file : files )
            {
                if ( storeFiles.contains( file ) )
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Read store ID from the metadata store.
     *
     * @param databaseLayout the database layout.
     * @param cursorTracer underlying page cursor tracer.
     * @return the store ID, never {@code null}.
     * @throws IOException if there is an error while reading the metadata store file.
     */
    public StoreId readStoreId( DatabaseLayout databaseLayout, PageCursorTracer cursorTracer ) throws IOException
    {
        return selectStorageEngine().storeId( databaseLayout, pageCache, cursorTracer );
    }
}
