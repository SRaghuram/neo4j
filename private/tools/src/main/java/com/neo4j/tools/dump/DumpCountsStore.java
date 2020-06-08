/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

/**
 * Tool that will dump content of count store content into a simple string representation for further analysis.
 */
public class DumpCountsStore
{
    public static void main( String[] args ) throws Exception
    {
        main( args, System.out );
    }

    public static void main( String[] args, PrintStream out ) throws Exception
    {
        if ( args.length != 1 )
        {
            System.err.println( "Expecting exactly one argument describing the path to the store" );
            System.exit( 1 );
        }
        Config config = defaults( pagecache_memory, "80M" );
        PageCacheTracer tracer = PageCacheTracer.NULL;
        NullLog log = NullLog.getInstance();
        VersionContextSupplier versions = EmptyVersionContextSupplier.EMPTY;
        SystemNanoClock clock = Clocks.nanoClock();
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
              JobScheduler scheduler = createInitialisedScheduler();
              PageCache pageCache = new ConfiguringPageCacheFactory( fs, config, tracer, log, versions, scheduler, clock, new MemoryPools() )
                      .getOrCreatePageCache() )
        {
            GBPTreeCountsStore.dump( pageCache, new File( args[0] ), out, NULL );
        }
    }
}
