/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;

public class StoreCopyProcess
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final ClusteredDatabaseContext clusteredDatabaseContext;
    private final Config config;
    private final CopiedStoreRecovery copiedStoreRecovery;
    private final Log log;
    private final RemoteStore remoteStore;

    public StoreCopyProcess( FileSystemAbstraction fs, PageCache pageCache, ClusteredDatabaseContext clusteredDatabaseContext,
            CopiedStoreRecovery copiedStoreRecovery, RemoteStore remoteStore, LogProvider logProvider )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.clusteredDatabaseContext = clusteredDatabaseContext;
        this.config = clusteredDatabaseContext.database().getConfig();
        this.copiedStoreRecovery = copiedStoreRecovery;
        this.remoteStore = remoteStore;
        this.log = logProvider.getLog( getClass() );
    }

    public void replaceWithStoreFrom( CatchupAddressProvider addressProvider, StoreId expectedStoreId )
            throws IOException, StoreCopyFailedException, DatabaseShutdownException
    {
        StorageEngineFactory storageEngineFactory = clusteredDatabaseContext.database().getStorageEngineFactory();
        try ( TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( fs, pageCache, clusteredDatabaseContext.databaseLayout(),
                storageEngineFactory ) )
        {
            //Check version compatibility before copy, just in case
            StoreVersion remoteStoreVersion;
            try
            {
                remoteStoreVersion = storageEngineFactory.versionInformation( expectedStoreId.getStoreVersion() );
            }
            catch ( Exception e )
            {
                throw new IllegalStateException( "Store copy failed due to unknown store version. " + expectedStoreId.getStoreVersion(), e );
            }

            if ( !copiedStoreRecovery.canRecoverRemoteStore( config, tempStore.databaseLayout(), remoteStoreVersion ) )
            {
                throw new IllegalStateException( "Store copy failed due to store version mismatch. The copied database will not be able to recover." +
                        " Copied store version " + remoteStoreVersion + " incompatible with current configuration." );
            }
            remoteStore.copy( addressProvider, expectedStoreId, tempStore.databaseLayout() );
            try
            {
                copiedStoreRecovery.recoverCopiedStore( config, tempStore.databaseLayout() );
            }
            catch ( Throwable e )
            {
                /*
                 * We keep the store until the next store copy attempt. If the exception
                 * is fatal then the store will stay around for potential forensics.
                 */
                tempStore.keepStore();
                throw e;
            }
            clusteredDatabaseContext.replaceWith( tempStore.databaseLayout().databaseDirectory() );
        }
        log.info( "Replaced store successfully" );
    }
}
