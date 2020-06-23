/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.junit.Assert.assertEquals;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

public class TestStoreId
{
    private TestStoreId()
    {
    }

    public static void assertAllStoresHaveTheSameStoreId( List<DatabaseLayout> databaseLayouts, FileSystemAbstraction fs ) throws Exception
    {
        Set<StoreId> storeIds = getStoreIds( databaseLayouts, fs );
        assertEquals( "Store Ids " + storeIds, 1, storeIds.size() );
    }

    public static Set<StoreId> getStoreIds( List<DatabaseLayout> databaseLayouts, FileSystemAbstraction fs ) throws Exception
    {
        Set<StoreId> storeIds = new HashSet<>();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, jobScheduler, PageCacheTracer.NULL ) )
        {
            for ( DatabaseLayout databaseLayout : databaseLayouts )
            {
                storeIds.add( doReadStoreId( databaseLayout, pageCache ) );
            }
        }

        return storeIds;
    }

    private static StoreId doReadStoreId( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        return MetaDataStore.getStoreId( pageCache, databaseLayout.metadataStore().toFile(), NULL );
    }
}
