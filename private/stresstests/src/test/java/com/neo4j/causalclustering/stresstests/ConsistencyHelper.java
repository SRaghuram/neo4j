/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import com.neo4j.causalclustering.catchup.storecopy.TemporaryStoreDirectory;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.common.Service;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;
import static org.neo4j.storageengine.api.StorageEngineFactory.selectStorageEngine;

final class ConsistencyHelper
{
    private ConsistencyHelper()
    {
    }

    static void assertStoreConsistent( FileSystemAbstraction fs, File tempStoreDir, DatabaseLayout databaseLayout ) throws Exception
    {
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, jobScheduler );
              TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( fs, pageCache, DatabaseLayout.of( tempStoreDir ) ) )
        {
            fs.copyRecursively( databaseLayout.databaseDirectory(), tempStore.storeDir() );

            new CopiedStoreRecovery( pageCache, fs, selectStorageEngine( Service.loadAll( StorageEngineFactory.class ) ) )
                    .recoverCopiedStore( Config.defaults(), tempStore.databaseLayout() );

            ConsistencyCheckService.Result result = runConsistencyCheckTool(
                    new String[]{databaseLayout.databaseDirectory().getAbsolutePath()},
                    new PrintStream( NULL_OUTPUT_STREAM ),
                    new PrintStream( NULL_OUTPUT_STREAM ) );

            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + databaseLayout.databaseDirectory() );
            }
        }
    }
}
