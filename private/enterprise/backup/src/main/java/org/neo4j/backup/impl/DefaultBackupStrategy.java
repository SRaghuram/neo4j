/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

class DefaultBackupStrategy extends LifecycleAdapter implements BackupStrategy
{
    private final BackupDelegator backupDelegator;
    private final AddressResolver addressResolver;
    private final Log log;
    private final StoreFiles storeFiles;

    DefaultBackupStrategy( BackupDelegator backupDelegator, AddressResolver addressResolver, LogProvider logProvider, StoreFiles storeFiles )
    {
        this.backupDelegator = backupDelegator;
        this.addressResolver = addressResolver;
        this.log = logProvider.getLog( DefaultBackupStrategy.class );
        this.storeFiles = storeFiles;
    }

    @Override
    public void performFullBackup( DatabaseLayout targetDbLayout, Config config, OptionalHostnamePort address ) throws BackupExecutionException
    {
        BackupInfo backupInfo = prepareForBackup( targetDbLayout, config, address );

        if ( backupInfo.localStoreId != null )
        {
            throw new BackupExecutionException( new StoreIdDownloadFailedException(
                    format( "Cannot perform a full backup onto preexisting backup. Remote store id was %s but local is %s",
                            backupInfo.remoteStoreId, backupInfo.localStoreId ) ) );
        }

        try
        {
            backupDelegator.copy( backupInfo.remoteAddress, backupInfo.remoteStoreId, targetDbLayout );
        }
        catch ( StoreCopyFailedException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    @Override
    public void performIncrementalBackup( DatabaseLayout targetDbLayout, Config config, OptionalHostnamePort address ) throws BackupExecutionException
    {
        BackupInfo backupInfo = prepareForBackup( targetDbLayout, config, address );

        if ( !Objects.equals( backupInfo.localStoreId, backupInfo.remoteStoreId ) )
        {
            throw new BackupExecutionException( new StoreIdDownloadFailedException(
                    format( "Remote store id was %s but local is %s", backupInfo.remoteStoreId, backupInfo.localStoreId ) ) );
        }

        catchup( backupInfo.remoteAddress, backupInfo.remoteStoreId, targetDbLayout );
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

    private BackupInfo prepareForBackup( DatabaseLayout databaseLayout, Config config, OptionalHostnamePort userProvidedAddress )
            throws BackupExecutionException
    {
        try
        {
            AdvertisedSocketAddress remoteAddress = addressResolver.resolveCorrectAddress( config, userProvidedAddress );
            log.info( "Resolved address for backup is " + remoteAddress );

            StoreId remoteStoreId = backupDelegator.fetchStoreId( remoteAddress );
            log.info( "Remote store id is " + remoteStoreId );

            StoreId localStoreId = readLocalStoreId( databaseLayout );
            log.info( "Local store id is " + remoteStoreId );

            return new BackupInfo( remoteAddress, remoteStoreId, localStoreId );
        }
        catch ( StoreIdDownloadFailedException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    private StoreId readLocalStoreId( DatabaseLayout databaseLayout )
    {
        try
        {
            return storeFiles.readStoreId( databaseLayout );
        }
        catch ( IOException e )
        {
            return null;
        }
    }

    private void catchup( AdvertisedSocketAddress fromAddress, StoreId storeId, DatabaseLayout databaseLayout ) throws BackupExecutionException
    {
        try
        {
            CatchupResult catchupResult = backupDelegator.tryCatchingUp( fromAddress, storeId, databaseLayout );
            if ( catchupResult != CatchupResult.SUCCESS_END_OF_STREAM )
            {
                throw new BackupExecutionException( new StoreCopyFailedException(
                        "End state of catchup was not a successful end of stream: " + catchupResult ) );
            }
        }
        catch ( StoreCopyFailedException e )
        {
            throw new BackupExecutionException( e );
        }
    }

    private static class BackupInfo
    {
        final AdvertisedSocketAddress remoteAddress;
        final StoreId remoteStoreId;
        final StoreId localStoreId;

        BackupInfo( AdvertisedSocketAddress remoteAddress, StoreId remoteStoreId, StoreId localStoreId )
        {
            this.remoteAddress = remoteAddress;
            this.remoteStoreId = remoteStoreId;
            this.localStoreId = localStoreId;
        }
    }
}
