/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.stresstests;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.TemporaryStoreDirectory;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.ThreadPoolJobScheduler;

import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

final class ConsistencyHelper
{
    private ConsistencyHelper()
    {
    }

    static void assertStoreConsistent( FileSystemAbstraction fs, File storeDir ) throws Exception
    {
        File parent = storeDir.getParentFile();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, jobScheduler );
              TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( fs, pageCache, parent ) )
        {
            fs.copyRecursively( storeDir, tempStore.storeDir() );

            new CopiedStoreRecovery( Config.defaults(), pageCache, fs )
                    .recoverCopiedStore( tempStore.databaseLayout() );

            ConsistencyCheckService.Result result = runConsistencyCheckTool(
                    new String[]{storeDir.getAbsolutePath()},
                    new PrintStream( NULL_OUTPUT_STREAM ),
                    new PrintStream( NULL_OUTPUT_STREAM ) );

            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + storeDir );
            }
        }
    }
}
