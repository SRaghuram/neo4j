/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
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
    public static final FilenameFilter EXCLUDE_TEMPORARY_DIRS = ( dir, name ) -> !name.equals( TEMP_STORE_COPY_DIRECTORY_NAME ) &&
                                                                                 !name.equals( TEMP_BOOTSTRAP_DIRECTORY_NAME ) &&
                                                                                 !name.equals( TEMP_SAVE_DIRECTORY_NAME );

    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final FilenameFilter filenameFilter;

    public StoreFiles( FileSystemAbstraction fs, PageCache pageCache )
    {
        this( fs, pageCache, EXCLUDE_TEMPORARY_DIRS );
    }

    public StoreFiles( FileSystemAbstraction fs, PageCache pageCache, FilenameFilter filenameFilter )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.filenameFilter = filenameFilter;
    }

    public void delete( DatabaseLayout databaseLayout, LogFiles logFiles ) throws IOException
    {
        File databaseDirectory = databaseLayout.databaseDirectory();
        File[] files = fs.listFiles( databaseDirectory, filenameFilter );
        if ( files != null )
        {
            for ( File file : files )
            {
                fs.deleteRecursively( file );
            }
        }

        for ( File txLog : logFiles.logFiles() )
        {
            fs.deleteFile( txLog );
        }
        fs.deleteFile( databaseDirectory );
    }

    public void delete( LogFiles logFiles )
    {
        for ( File txLog : logFiles.logFiles() )
        {
            fs.deleteFile( txLog );
        }
    }

    public void moveTo( File source, DatabaseLayout target, LogFiles logFiles ) throws IOException
    {
        fs.mkdirs( logFiles.logFilesDirectory() );

        File[] files = fs.listFiles( source, filenameFilter );
        if ( files != null )
        {
            for ( File file : files )
            {
                File destination = logFiles.isLogFile( file ) ? target.getTransactionLogsDirectory() : target.databaseDirectory();
                fs.moveToDirectory( file, destination );
            }
        }
    }

    public boolean isEmpty( DatabaseLayout databaseLayout )
    {
        Set<File> storeFiles = databaseLayout.storeFiles();

        File[] files = fs.listFiles( databaseLayout.databaseDirectory() );
        if ( files != null )
        {
            for ( File file : files )
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
