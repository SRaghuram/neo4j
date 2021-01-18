/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.local;

import com.neo4j.backup.impl.MetadataStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.backup.impl.local.DatabaseIdStore.getDatabaseFilePath;
import static java.lang.String.format;

public class BackupLocation
{
    private final DatabaseLayout databaseLayout;
    private final StoreFiles storeFiles;
    private final DatabaseIdStore databaseIdStore;
    private final MetadataStore metadataStore;
    private final PageCacheTracer pageCacheTracer;
    private final FileManager fileManager;
    private final Log log;
    private final LogProvider logProvider;

    public BackupLocation( DatabaseLayout databaseLayout,
                           StoreFiles storeFiles,
                           DatabaseIdStore databaseIdStore,
                           MetadataStore metadataStore,
                           PageCacheTracer pageCacheTracer,
                           FileManager fileManager,
                           LogProvider logProvider )
    {
        this.databaseLayout = databaseLayout;
        this.storeFiles = storeFiles;
        this.databaseIdStore = databaseIdStore;
        this.metadataStore = metadataStore;
        this.pageCacheTracer = pageCacheTracer;
        this.fileManager = fileManager;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    public DatabaseLayout databaseLayout()
    {
        return databaseLayout;
    }

    public boolean isLocatedAt( BackupLocation location )
    {
        var thisDirectory = databaseDirectory().toAbsolutePath();
        var thatDirectory = location.databaseDirectory().toAbsolutePath();

        return thisDirectory.equals( thatDirectory );
    }

    public Path databaseDirectory()
    {
        return databaseLayout.databaseDirectory();
    }

    public boolean hasSameStore( BackupLocation thatBackupLocation ) throws IOException
    {
        var thisStoreIdOpt = storeId();

        if ( thisStoreIdOpt.isPresent() )
        {
            var thisStoreId = thisStoreIdOpt.get();
            try
            {
                var thatStoreIdOpt = thatBackupLocation.storeId();
                if ( thatStoreIdOpt.isPresent() )
                {
                    var thatStoreId = thatStoreIdOpt.get();
                    return thisStoreId.equals( thatStoreId );
                }
            }
            catch ( IllegalStateException ex )
            {
                // no  store id
            }
        }

        return false;
    }

    public Optional<StoreId> storeId() throws IOException
    {
        if ( !hasExistingStore() )
        {
            return Optional.empty();
        }

        checkValidStore( fileManager, databaseLayout );

        try ( var tracer = pageCacheTracer.createPageCursorTracer( "backup-location" ) )
        {
            return Optional.ofNullable( storeFiles.readStoreId( databaseLayout, tracer ) );
        }
    }

    private static void checkValidStore( FileManager fileManager, DatabaseLayout databaseLayout )
    {
        if ( !fileManager.exists( databaseLayout.metadataStore() ) )
        {
            throw new IllegalStateException( format( "Destination directory '%s' exists and is not empty but does not contain previous backup",
                                                     databaseLayout.databaseDirectory() ) );
        }
    }

    public boolean hasExistingStore()
    {
        return !fileManager.directoryDoesNotExistOrIsEmpty( databaseLayout.databaseDirectory() );
    }

    public void writeMetadata( List<String> metaData ) throws IOException
    {
        metadataStore.write( databaseLayout().backupToolsFolder(), metaData );
    }

    public boolean tryDelete()
    {
        var dir = databaseDirectory();
        try
        {
            fileManager.deleteDir( dir );
        }
        catch ( IOException ex )
        {
            log.warn( "Failed to delete dir %s", dir, ex );
            return false;
        }
        return true;
    }

    public BackupLocation moveTo( BackupLocation userBackupLocation ) throws IOException
    {
        return moveTo( userBackupLocation.databaseDirectory() );
    }

    public BackupLocation moveTo( Path to ) throws IOException
    {
        fileManager.copyDelete( databaseDirectory(), to );
        var databaseLayout = DatabaseLayout.ofFlat( to );
        return new BackupLocation( databaseLayout, storeFiles, databaseIdStore, metadataStore, pageCacheTracer, fileManager, logProvider );
    }

    public boolean conflictsWith( DatabaseId databaseId )
    {
        if ( databaseId().isEmpty() )
        {
            return false;
        }
        else
        {
            return databaseId().filter( id -> id.equals( databaseId ) ).isEmpty();
        }
    }

    public Optional<DatabaseId> databaseId()
    {
        final var path = databaseLayout.backupToolsFolder();
        try
        {
            return databaseIdStore.readDatabaseId( path );
        }
        catch ( Exception exception )
        {
            log.error( "Error in reading databaseId file " + getDatabaseFilePath( path ), exception );
            return Optional.empty();
        }
    }

    public void writeDatabaseId( DatabaseId databaseId ) throws IOException
    {
        databaseIdStore.writeDatabaseId( databaseId, databaseLayout.backupToolsFolder() );
    }

    @Override
    public String toString()
    {
        return "BackupLocation{" +
               "databaseLayout=" + databaseLayout +
               '}';
    }

    public boolean hasStoreId( StoreId thatStoreId ) throws IOException
    {
        return storeId().filter( thisStoreId -> thisStoreId.equals( thatStoreId ) ).isPresent();
    }

    public boolean hasDatabaseId( DatabaseId thatDatabaseId )
    {
        return databaseId().stream().allMatch( thisDatabaseId -> thisDatabaseId.equals( thatDatabaseId ) );
    }
}
