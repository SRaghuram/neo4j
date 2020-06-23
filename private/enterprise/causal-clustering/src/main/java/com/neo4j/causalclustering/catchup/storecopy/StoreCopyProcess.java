/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.storageengine.api.StoreId;

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
        try ( TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( fs, pageCache, clusteredDatabaseContext.databaseLayout(),
                clusteredDatabaseContext.database().getStorageEngineFactory() ) )
        {
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
            clusteredDatabaseContext.replaceWith( tempStore.databaseLayout().databaseDirectory().toFile() );
        }
        log.info( "Replaced store successfully" );
    }
}
