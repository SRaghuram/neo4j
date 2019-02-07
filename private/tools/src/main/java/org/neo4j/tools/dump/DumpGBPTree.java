/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.PrintingGBPTreeVisitor;
import org.neo4j.index.internal.gbptree.GBPTreeStructure;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.graphdb.config.Configuration.EMPTY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

/**
 * For now only dumps header, could be made more useful over time.
 */
public class DumpGBPTree
{
    /**
     * Dumps stuff about a {@link GBPTree} to console in human readable format.
     *
     * @param args arguments.
     * @throws IOException on I/O error.
     */
    public static void main( String[] args ) throws Exception
    {
        if ( args.length == 0 )
        {
            System.err.println( "File argument expected" );
            System.exit( 1 );
        }

        File file = new File( args[0] );
        System.out.println( "Dumping " + file.getAbsolutePath() );

        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = pageCache( jobScheduler ) )
        {
            PrintingGBPTreeVisitor visitor = new PrintingGBPTreeVisitor( System.out, false, false, false, false );
            GBPTreeStructure.visitHeader( pageCache, file, visitor );
        }
    }

    private static PageCache pageCache( JobScheduler jobScheduler )
    {
        SingleFilePageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        swapper.open( fs, EMPTY );
        PageCursorTracerSupplier cursorTracerSupplier = PageCursorTracerSupplier.NULL;
        return new MuninnPageCache( swapper, 100, NULL, cursorTracerSupplier, EmptyVersionContextSupplier.EMPTY, jobScheduler );
    }
}
