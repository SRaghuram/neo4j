/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class TransactionLogCatchUpFactory
{
    public TransactionLogCatchUpWriter create( DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache, Config config,
            LogProvider logProvider, StorageEngineFactory storageEngineFactory, LongRange validInitialTx, boolean fullStoreCopy,
            boolean keepTxLogsInStoreDir, PageCacheTracer pageCacheTracer ) throws IOException
    {
        return new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, logProvider, storageEngineFactory, validInitialTx,
                fullStoreCopy, keepTxLogsInStoreDir, pageCacheTracer );
    }
}
