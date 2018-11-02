/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

public class TransactionLogCatchUpFactory
{
    public TransactionLogCatchUpWriter create( DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache,
            Config config, LogProvider logProvider, long fromTxId, boolean asPartOfStoreCopy, boolean keepTxLogsInStoreDir,
            boolean rotateTransactionsManually )
            throws IOException
    {
        return new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, logProvider, fromTxId,
                asPartOfStoreCopy, keepTxLogsInStoreDir, rotateTransactionsManually );
    }
}
