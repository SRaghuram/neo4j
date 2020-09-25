/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.storecopy.DatabaseIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static java.lang.String.format;

class DefaultBackupStrategy extends LifecycleAdapter implements BackupStrategy
{
    private static final String BACKUP_LOCAL_STORE_READER_TAG = "backupLocalStoreReader";
    private final BackupDelegator backupDelegator;
    private final DatabaseIdStore databaseIdStore;
    private final Log log;
    private final StoreFiles storeFiles;
    private final PageCacheTracer pageCacheTracer;

    DefaultBackupStrategy( BackupDelegator backupDelegator,
                           LogProvider logProvider,
                           StoreFiles storeFiles,
                           PageCacheTracer pageCacheTracer,
                           DatabaseIdStore databaseIdStore )
    {
        this.backupDelegator = backupDelegator;
        this.log = logProvider.getLog( DefaultBackupStrategy.class );
        this.storeFiles = storeFiles;
        this.pageCacheTracer = pageCacheTracer;
        this.databaseIdStore = databaseIdStore;
    }

    @Override
    public void performFullBackup( DatabaseLayout targetDbLayout, SocketAddress address, String databaseName ) throws BackupExecutionException
    {
        BackupInfo backupInfo = prepareForBackup( targetDbLayout, address, databaseName );

        if ( backupInfo.localStoreId != null )
        {
            throw new BackupExecutionException( new StoreIdDownloadFailedException(
                    format( "Cannot perform a full backup onto preexisting backup. Remote store id was %s but local is %s",
                            backupInfo.remoteStoreId, backupInfo.localStoreId ) ) );
        }

        try
        {
            backupDelegator.copy( backupInfo.remoteAddress, backupInfo.remoteStoreId, backupInfo.namedDatabaseId, targetDbLayout );
            writeDatabaseId( targetDbLayout.databaseDirectory(), backupInfo.namedDatabaseId.databaseId() );
        }
        catch ( StoreCopyFailedException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    @Override
    public void performIncrementalBackup( DatabaseLayout targetDbLayout, SocketAddress address, String databaseName ) throws BackupExecutionException
    {
        BackupInfo backupInfo = prepareForBackup( targetDbLayout, address, databaseName );

        if ( !Objects.equals( backupInfo.localStoreId, backupInfo.remoteStoreId ) )
        {
            throw new BackupExecutionException( new StoreIdDownloadFailedException(
                    format( "Remote store id was %s but local is %s", backupInfo.remoteStoreId, backupInfo.localStoreId ) ) );
        }
        checkIsTheSameDatabaseId( targetDbLayout, backupInfo.namedDatabaseId.databaseId() );

        catchup( backupInfo.remoteAddress, backupInfo.remoteStoreId, backupInfo.namedDatabaseId, targetDbLayout );

        writeDatabaseId( targetDbLayout.databaseDirectory(), backupInfo.namedDatabaseId.databaseId() );
    }

    @Override
    public void start()
    {
        backupDelegator.start();
    }

    @Override
    public void stop()
    {
        backupDelegator.stop();
    }

    private BackupInfo prepareForBackup( DatabaseLayout databaseLayout, SocketAddress address, String databaseName ) throws BackupExecutionException
    {
        try
        {
            log.info( "Remote backup address is " + address );

            NamedDatabaseId namedDatabaseId = backupDelegator.fetchDatabaseId( address, databaseName );
            log.info( "Database id is " + namedDatabaseId );

            StoreId remoteStoreId = backupDelegator.fetchStoreId( address, namedDatabaseId );
            log.info( "Remote store id is " + remoteStoreId );

            StoreId localStoreId = readLocalStoreId( databaseLayout, pageCacheTracer );
            log.info( "Local store id is " + remoteStoreId );

            return new BackupInfo( address, remoteStoreId, localStoreId, namedDatabaseId );
        }
        catch ( StoreIdDownloadFailedException | DatabaseIdDownloadFailedException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    private StoreId readLocalStoreId( DatabaseLayout databaseLayout, PageCacheTracer pageCacheTracer )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( BACKUP_LOCAL_STORE_READER_TAG ) )
        {
            if ( storeFiles.isEmpty( databaseLayout ) )
            {
                return null;
            }
            return storeFiles.readStoreId( databaseLayout, cursorTracer );
        }
        catch ( IOException e )
        {
            log.warn( "Unable to read store ID from metadata store in " + databaseLayout, e );
            return null;
        }
    }

    private void catchup( SocketAddress fromAddress, StoreId storeId, NamedDatabaseId namedDatabaseId, DatabaseLayout databaseLayout )
            throws BackupExecutionException
    {
        try
        {
            backupDelegator.tryCatchingUp( fromAddress, storeId, namedDatabaseId, databaseLayout );
        }
        catch ( StoreCopyFailedException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    private void checkIsTheSameDatabaseId( DatabaseLayout databaseLayout, DatabaseId expectedDatabaseId ) throws BackupExecutionException
    {
        final var databaseId = databaseIdStore.readDatabaseId( databaseLayout.databaseDirectory() );
        if ( databaseId.isPresent() && !databaseId.get().equals( expectedDatabaseId ) )
        {
            final var message = format( "DatabaseId %s stored on the file system doesn't match with the server one %s", databaseId.get().uuid(),
                                        expectedDatabaseId.uuid() );
            throw new BackupExecutionException( new IllegalStateException( message ) );
        }
    }

    private void writeDatabaseId( Path folderPath, DatabaseId databaseId ) throws BackupExecutionException
    {
        try
        {
            databaseIdStore.writeDatabaseId( databaseId, folderPath );
        }
        catch ( IOException e )
        {
            throw new BackupExecutionException(
                    format( "Can't write the databaseId=%s in %s", databaseId, folderPath ) );
        }
    }

    private static class BackupInfo
    {
        final SocketAddress remoteAddress;
        final StoreId remoteStoreId;
        final StoreId localStoreId;
        final NamedDatabaseId namedDatabaseId;

        BackupInfo( SocketAddress remoteAddress, StoreId remoteStoreId, StoreId localStoreId, NamedDatabaseId namedDatabaseId )
        {
            this.remoteAddress = remoteAddress;
            this.remoteStoreId = remoteStoreId;
            this.localStoreId = localStoreId;
            this.namedDatabaseId = namedDatabaseId;
        }
    }
}
