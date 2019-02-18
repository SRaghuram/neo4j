/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.common.LocalDatabase;
import com.neo4j.causalclustering.identity.StoreId;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class StoreCopyProcess
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final LocalDatabase localDatabase;
    private final Config config;
    private final CopiedStoreRecovery copiedStoreRecovery;
    private final Log log;
    private final RemoteStore remoteStore;

    public StoreCopyProcess( FileSystemAbstraction fs, PageCache pageCache, LocalDatabase localDatabase,
            CopiedStoreRecovery copiedStoreRecovery, RemoteStore remoteStore, LogProvider logProvider )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.localDatabase = localDatabase;
        this.config = localDatabase.database().getConfig();
        this.copiedStoreRecovery = copiedStoreRecovery;
        this.remoteStore = remoteStore;
        this.log = logProvider.getLog( getClass() );
    }

    public void replaceWithStoreFrom( CatchupAddressProvider addressProvider, StoreId expectedStoreId )
            throws IOException, StoreCopyFailedException, DatabaseShutdownException
    {
        try ( TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( fs, pageCache, localDatabase.databaseLayout() ) )
        {
            remoteStore.copy( addressProvider, expectedStoreId, tempStore.databaseLayout(), false );
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
            localDatabase.replaceWith( tempStore.databaseLayout().databaseDirectory() );
        }
        log.info( "Replaced store successfully" );
    }
}
