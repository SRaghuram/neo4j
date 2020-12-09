/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;

public class DumpIdGenerator
{
    public static void main( String[] args ) throws IOException
    {
        if ( args.length == 0 )
        {
            System.err.println( "Please provide id file to dump" );
            return;
        }

        Path file = Path.of( args[0] );
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        SingleFilePageSwapperFactory swapper = new SingleFilePageSwapperFactory( fs );
        LifeSupport life = new LifeSupport();
        JobScheduler scheduler = life.add( JobSchedulerFactory.createScheduler() );
        life.start();
        var pageCacheTracer = PageCacheTracer.NULL;
        try ( MuninnPageCache pageCache = new MuninnPageCache( swapper, scheduler, MuninnPageCache.config( 1_000 ) ) )
        {
            IndexedIdGenerator.dump( pageCache, file, pageCacheTracer );
        }
        finally
        {
            life.shutdown();
        }
    }
}
