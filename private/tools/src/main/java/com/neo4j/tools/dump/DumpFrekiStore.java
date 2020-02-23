/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.freki.FrekiAnalysis;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.Lifespan;

import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier.EMPTY;

public class DumpFrekiStore
{
    public static void main( String[] arguments ) throws IOException
    {
        var args = Args.parse( arguments );
        var databaseLayout = DatabaseLayout.ofFlat( new File( args.orphans().get( 0 ) ) );
        var fs = new DefaultFileSystemAbstraction();
        var scheduler = JobSchedulerFactory.createScheduler();
        try ( var life = new Lifespan( scheduler );
              var pageCache = new MuninnPageCache( new SingleFilePageSwapperFactory( fs ), 10_000, NULL, EMPTY, scheduler );
              var storeLife = new Lifespan() )
        {
            var analysis = new FrekiAnalysis( fs, databaseLayout, pageCache );
            var nodeIdSpec = args.get( "nodeId", null );
            if ( nodeIdSpec != null )
            {
                analysis.dumpNodes( nodeIdSpec );
            }
            var stats = args.getBoolean( "stats", false );
            if ( stats )
            {
                analysis.dumpStats();
            }
        }
    }
}
