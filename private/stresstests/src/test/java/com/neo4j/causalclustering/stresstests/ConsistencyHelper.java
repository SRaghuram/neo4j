/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.NullLogProvider;

final class ConsistencyHelper
{
    private static final ConsistencyFlags CONSISTENCY_FLAGS = new ConsistencyFlags( true, true, true, true, true, true );

    private ConsistencyHelper()
    {
    }

    static void assertStoreConsistent( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache ) throws Exception
    {
        ConsistencyCheckService.Result result =
                new ConsistencyCheckService().runFullConsistencyCheck( databaseLayout, Config.defaults(), ProgressMonitorFactory.NONE,
                        NullLogProvider.getInstance(), fs, pageCache, false, CONSISTENCY_FLAGS, PageCacheTracer.NULL );

        if ( !result.isSuccessful() )
        {
            throw new RuntimeException( "Not consistent database in " + databaseLayout.databaseDirectory() );
        }
    }
}
