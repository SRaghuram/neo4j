/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.local;

import com.neo4j.backup.impl.MetadataStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class LocationManager
{

    private final StoreFiles storeFiles;
    private final DatabaseLayout userBackupLocation;
    private final PageCacheTracer pageCacheTracer;
    private final Log log;
    private final DatabaseIdStore databaseIdStore;
    private final FileManager fileManager;
    private final MetadataStore metadataStore;
    private final LogProvider logProvider;

    public LocationManager( FileSystemAbstraction fs, PageCache pageCache, Path backupLocation, String databaseName, PageCacheTracer pageCacheTracer,
                            LogProvider logProvider )
    {
        this.userBackupLocation = DatabaseLayout.ofFlat( backupLocation.resolve( databaseName ) );
        this.pageCacheTracer = pageCacheTracer;
        storeFiles = new StoreFiles( fs, pageCache );
        log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
        databaseIdStore = new DatabaseIdStore( fs );
        metadataStore = new MetadataStore( fs );
        fileManager = new FileManager( fs, 10 );
    }

    public BackupLocation createBackupLocation()
    {
        return new BackupLocation( userBackupLocation, storeFiles, databaseIdStore, metadataStore, pageCacheTracer, fileManager, logProvider );
    }

    public BackupLocation createTemporaryEmptyLocation()
    {
        var tmpDir = fileManager.nextWorkingDir( userBackupLocation.databaseDirectory() );
        var tmpDatabaseLayout = DatabaseLayout.ofFlat( tmpDir );
        return new BackupLocation( tmpDatabaseLayout, storeFiles, databaseIdStore, metadataStore, pageCacheTracer, fileManager, logProvider );
    }

    public void reportSuccessful( BackupLocation actualBackupLocation, DatabaseId databaseId ) throws IOException
    {
        BackupLocation userBackupLocation = moveToUserBackupLocation( actualBackupLocation, databaseId );

        try
        {
            userBackupLocation.writeDatabaseId( databaseId );
        }
        catch ( IOException e )
        {
            log.warn( "Failed to write database id to backup", e );
        }
    }

    private BackupLocation moveToUserBackupLocation( BackupLocation actualBackupLocation, DatabaseId databaseId ) throws IOException
    {
        var userBackupLocation = createBackupLocation();

        if ( !actualBackupLocation.isLocatedAt( userBackupLocation ) )
        {
            boolean deleted = false;
            if ( actualBackupLocation.hasSameStore( userBackupLocation ) )
            {
                deleted = userBackupLocation.tryDelete();
            }

            if ( !deleted )
            {
                var errorDir = fileManager.nextErrorDir( userBackupLocation.databaseDirectory() );
                log.info( "Moving previous backup content in %s to errorDir %s", userBackupLocation, errorDir );
                userBackupLocation.moveTo( errorDir );
            }
            actualBackupLocation.moveTo( userBackupLocation );
        }

        if ( actualBackupLocation.conflictsWith( databaseId ) )
        {
            var actualDatabaseId = actualBackupLocation.databaseId()
                                                       .map( Object::toString )
                                                       .orElse( "<no-db>" );
            var msg = format( "Miss-match in database id. Existing backup has %s, trying to write %s", actualDatabaseId, databaseId );
            throw new IllegalArgumentException( msg );
        }
        return userBackupLocation;
    }

    public void writeMetaData( BackupLocation backupLocation, List<String> metaData ) throws IOException
    {
        backupLocation.writeMetadata( metaData );
    }
}
