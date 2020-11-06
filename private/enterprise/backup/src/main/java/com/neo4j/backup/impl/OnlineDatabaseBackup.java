/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.backup.impl.local.BackupLocation;
import com.neo4j.backup.impl.local.LocationManager;
import com.neo4j.backup.impl.remote.BackupClient;
import com.neo4j.backup.impl.remote.RemoteInfo;
import com.neo4j.backup.impl.tools.BackupConsistencyChecker;
import com.neo4j.backup.impl.tools.BackupRecoveryService;
import com.neo4j.backup.impl.tools.ConsistencyCheckExecutionException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public final class OnlineDatabaseBackup
{
    private final FileSystemAbstraction fs;
    private final LogProvider internalLogProvider;

    private final Log userLog;

    public OnlineDatabaseBackup( FileSystemAbstraction fs, LogProvider internalLogProvider, Log userLog )
    {

        this.fs = fs;
        this.internalLogProvider = internalLogProvider;
        this.userLog = userLog;
    }

    public void backup( OnlineBackupContext context,
                        PageCacheTracer pageCacheTracer,
                        BackupConsistencyChecker consistencyChecker,
                        BackupsLifecycle lifecycle,
                        BackupRecoveryService recoveryService,
                        BackupClient backupClient,
                        String databaseName,
                        BackupOutputMonitor backupMonitor )
            throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        try
        {
            backupMonitor.start();
            var locationManager =
                    new LocationManager( fs, lifecycle.getPageCache(), context.getBackupDir(), databaseName, pageCacheTracer, internalLogProvider );

            var remoteInfo = backupClient.prepareToBackup( context.getAddress(), databaseName );

            var backupLocation = retrieveRemoteStore( context.fallbackToFullBackupEnabled(), backupClient, locationManager, remoteInfo, backupMonitor );

            writeMetadata( context, backupClient, databaseName, locationManager, backupLocation );

            backupMonitor.startRecoveringStore();
            recoveryService.recover( backupLocation.databaseLayout() );
            backupMonitor.finishRecoveringStore();
            consistencyChecker.checkConsistency( backupLocation.databaseLayout() );

            locationManager.reportSuccessful( backupLocation, remoteInfo.namedDatabaseId().databaseId() );
        }
        catch ( ConsistencyCheckExecutionException | BackupExecutionException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new BackupExecutionException( e );
        }
        finally
        {
            backupMonitor.finish();
        }
    }

    private void writeMetadata( OnlineBackupContext context, BackupClient backupClient, String databaseName, LocationManager locationManager,
                                BackupLocation backupLocation ) throws Exception
    {
        if ( context.getIncludeMetadata().isPresent() )
        {
            if ( !databaseName.equals( SYSTEM_DATABASE_NAME ) )
            {
                var metaData = backupClient.downloadMetaData( context.getAddress(), databaseName, context.getIncludeMetadata().get() );
                locationManager.writeMetaData( backupLocation, metaData );
            }
            else
            {
                userLog.warn( "Include metadata parameter is invalid for backing up system database" );
            }
        }
    }

    private BackupLocation retrieveRemoteStore( boolean fallBackToFull, BackupClient backupClient, LocationManager locationManager, RemoteInfo remoteInfo,
                                                BackupOutputMonitor backupMonitor )
            throws Exception
    {
        var backupLocation = locationManager.createBackupLocation();

        if ( backupLocation.hasExistingStore() )
        {
            var didIncrementalUpdate = incrementalStoreUpdate( fallBackToFull, backupClient, remoteInfo, backupLocation, backupMonitor );
            if ( didIncrementalUpdate )
            {
                return backupLocation;
            }
            backupLocation = locationManager.createTemporaryEmptyLocation();
        }
        backupClient.fullStoreCopy( remoteInfo, backupLocation.databaseLayout(), backupMonitor );
        return backupLocation;
    }

    private boolean incrementalStoreUpdate( boolean fallBackToFull, BackupClient backupClient, RemoteInfo remoteInfo, BackupLocation backupLocation,
                                            BackupOutputMonitor backupMonitor )
            throws StoreIdDownloadFailedException, StoreCopyFailedException, IOException
    {
        try
        {
            checkStoreId( remoteInfo, backupLocation );
            checkDatabaseId( remoteInfo, backupLocation );
            backupClient.updateStore( remoteInfo, backupLocation.databaseLayout(), backupMonitor );
            return true;
        }
        catch ( Exception e )
        {
            if ( !fallBackToFull )
            {
                throw e;
            }
            return false;
        }
    }

    private void checkDatabaseId( RemoteInfo remoteInfo, BackupLocation backupLocation )
    {
        var databaseId = remoteInfo.namedDatabaseId().databaseId();
        if ( !backupLocation.hasDatabaseId( databaseId ) )
        {
            var dbIdName = backupLocation.databaseId()
                                         .map( DatabaseId::uuid )
                                         .map( Object::toString )
                                         .orElse( "<no-database-id>" );
            throw new IllegalStateException(
                    format( "DatabaseId %s stored on the file system doesn't match with the server one %s", dbIdName, databaseId.uuid() ) );
        }
    }

    private void checkStoreId( RemoteInfo remoteInfo, BackupLocation backupLocation ) throws IOException, StoreIdDownloadFailedException
    {
        var hasSameStoreIds = !backupLocation.hasStoreId( remoteInfo.storeId() );
        if ( hasSameStoreIds )
        {
            throw new StoreIdDownloadFailedException( "Remote store and existing backup have different store ids!" );
        }
    }
}
