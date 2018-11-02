/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.ThreadPoolJobScheduler;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_ID;

public class TestStoreId
{
    private TestStoreId()
    {
    }

    public static void assertAllStoresHaveTheSameStoreId( List<File> coreStoreDirs, FileSystemAbstraction fs ) throws Exception
    {
        Set<StoreId> storeIds = getStoreIds( coreStoreDirs, fs );
        assertEquals( "Store Ids " + storeIds, 1, storeIds.size() );
    }

    public static Set<StoreId> getStoreIds( List<File> coreStoreDirs, FileSystemAbstraction fs ) throws Exception
    {
        Set<StoreId> storeIds = new HashSet<>();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, jobScheduler ) )
        {
            for ( File coreStoreDir : coreStoreDirs )
            {
                storeIds.add( doReadStoreId( coreStoreDir, pageCache ) );
            }
        }

        return storeIds;
    }

    private static StoreId doReadStoreId( File databaseDirectory, PageCache pageCache ) throws IOException
    {
        File metadataStore = DatabaseLayout.of( databaseDirectory ).metadataStore();

        long creationTime = MetaDataStore.getRecord( pageCache, metadataStore, TIME );
        long randomNumber = MetaDataStore.getRecord( pageCache, metadataStore, RANDOM_NUMBER );
        long upgradeTime = MetaDataStore.getRecord( pageCache, metadataStore, UPGRADE_TIME );
        long upgradeId = MetaDataStore.getRecord( pageCache, metadataStore, UPGRADE_TRANSACTION_ID );

        return new StoreId( creationTime, randomNumber, upgradeTime, upgradeId );
    }
}
