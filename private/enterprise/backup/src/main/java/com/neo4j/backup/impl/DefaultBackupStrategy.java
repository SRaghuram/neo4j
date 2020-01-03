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
import java.util.Objects;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static java.lang.String.format;

class DefaultBackupStrategy extends LifecycleAdapter implements BackupStrategy
{
    private final BackupDelegator backupDelegator;
    private final Log log;
    private final StoreFiles storeFiles;

    DefaultBackupStrategy( BackupDelegator backupDelegator, LogProvider logProvider, StoreFiles storeFiles )
    {
        this.backupDelegator = backupDelegator;
        this.log = logProvider.getLog( DefaultBackupStrategy.class );
        this.storeFiles = storeFiles;
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

        catchup( backupInfo.remoteAddress, backupInfo.remoteStoreId, backupInfo.namedDatabaseId, targetDbLayout );
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

            StoreId localStoreId = readLocalStoreId( databaseLayout );
            log.info( "Local store id is " + remoteStoreId );

            return new BackupInfo( address, remoteStoreId, localStoreId, namedDatabaseId );
        }
        catch ( StoreIdDownloadFailedException | DatabaseIdDownloadFailedException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    private StoreId readLocalStoreId( DatabaseLayout databaseLayout )
    {
        try
        {
            if ( storeFiles.isEmpty( databaseLayout ) )
            {
                return null;
            }
            return storeFiles.readStoreId( databaseLayout );
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
