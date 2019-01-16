/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport.backup_stores;

import java.io.File;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class EmptyBackupStoreWithoutTransactionLogs extends EmptyBackupStore
{
    @Override
    void modify( File backup ) throws Exception
    {
        deleteTransactionLogs( backup );
        resetLastCommittedTransactionIdInMetadataStore( backup );
    }

    private static void resetLastCommittedTransactionIdInMetadataStore( File dir )
    {
        File metadataStore = DatabaseLayout.of( dir ).metadataStore();

        try ( JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( new DefaultFileSystemAbstraction(), jobScheduler ) )
        {
            // set the last committed transaction id to the base transaction id, meaning no transactions were committed
            MetaDataStore.setRecord( pageCache, metadataStore, LAST_TRANSACTION_ID, BASE_TX_ID );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
