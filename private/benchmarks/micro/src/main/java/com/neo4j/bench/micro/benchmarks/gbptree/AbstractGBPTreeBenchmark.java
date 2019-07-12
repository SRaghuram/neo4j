/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.data.Augmenterizer;
import com.neo4j.bench.micro.data.Stores.StoreAndConfig;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;

public abstract class AbstractGBPTreeBenchmark extends BaseDatabaseBenchmark
{
    private static final String INDEX_FILE = "gbptree";
    private static final long SEED = 42L;

    private PageCache pageCache;
    private File indexFile;
    GBPTree<AdaptableKey,AdaptableValue> gbpTree;
    AdaptableLayout layout;

    abstract Layout layout();

    abstract int keySize();

    abstract int valueSize();

    static long assertCount( long count, long expectedCount )
    {
        if ( count != expectedCount )
        {
            throw new RuntimeException( "Expected " + expectedCount + " but found " + count );
        }
        return count;
    }

    @Override
    public String benchmarkGroup()
    {
        return "GBPTree";
    }

    @Override
    protected StartDatabaseInstruction afterDataGeneration()
    {
        try
        {
            indexFile = managedStore.store().graphDbDirectory().resolve( INDEX_FILE ).toFile();
            layout = layout().create( keySize(), valueSize() );
            pageCache = createPageCache( indexFile );
            gbpTree = createGBPTree( pageCache, indexFile, layout );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        // do not start store
        return StartDatabaseInstruction.DO_NOT_START_DB;
    }

    @Override
    protected Augmenterizer augmentDataGeneration()
    {
        return new Augmenterizer()
        {
            @Override
            public void augment( int threads, StoreAndConfig storeAndConfig )
            {
                indexFile = storeAndConfig.store().graphDbDirectory().resolve( INDEX_FILE ).toFile();
                layout = layout().create( keySize(), valueSize() );
                try ( PageCache pageCache = createPageCache( indexFile );
                      GBPTree<AdaptableKey,AdaptableValue> gbpTree = createGBPTree(
                              pageCache,
                              indexFile,
                              layout ) )
                {
                    generateInitialTree( gbpTree, layout );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public String augmentKey( FullBenchmarkName benchmarkName )
            {
                System.out.println( "KEY: " + benchmarkName.name() );
                return benchmarkName.name();
            }
        };
    }

    private void generateInitialTree( GBPTree<AdaptableKey,AdaptableValue> gbpTree, AdaptableLayout layout ) throws IOException
    {
        AdaptableKey key = layout.newKey();
        AdaptableValue value = layout.newValue();
        try ( Writer<AdaptableKey,AdaptableValue> writer = gbpTree.writer() )
        {
            long initialTreeSize = initialTreeSize();
            Random random = randomSequence( 0 );
            ProgressListener progress = ProgressMonitorFactory
                    .textual( System.out )
                    .singlePart( "BuildTree", initialTreeSize );
            for ( long i = 0; i < initialTreeSize; i++ )
            {
                long seed = random.nextLong();
                layout.keyWithSeed( key, seed );
                layout.valueWithSeed( value, seed );
                writer.put( key, value );
                if ( (i + 1) % 10_000 == 0 )
                {
                    progress.add( 10_000 );
                }
            }
        }
        gbpTree.checkpoint( IOLimiter.UNLIMITED );
    }

    static Random randomSequence( long pos )
    {
        Random random = new Random( SEED );
        for ( long i = 0; i < pos; i++ )
        {
            random.nextLong();
        }
        return random;
    }

    private static PageCache createPageCache( File indexFile ) throws IOException
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        fs.mkdirs( indexFile.getParentFile() );
        Config config = Config.defaults();
        PageCacheTracer tracer = new DefaultPageCacheTracer();
        Log log = NullLog.getInstance();
        PageCursorTracerSupplier tracerSupplier = DefaultPageCursorTracerSupplier.NULL;
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                tracer,
                tracerSupplier,
                log,
                EmptyVersionContextSupplier.EMPTY,
                JobSchedulerFactory.createInitialisedScheduler() );
        return factory.getOrCreatePageCache();
    }

    private static GBPTree<AdaptableKey,AdaptableValue> createGBPTree(
            PageCache pageCache,
            File indexFile,
            AdaptableLayout layout )
    {
        return new GBPTree<>(
                pageCache,
                indexFile,
                layout,
                pageCache.pageSize(),
                NO_MONITOR,
                NO_HEADER_READER,
                NO_HEADER_WRITER,
                RecoveryCleanupWorkCollector.immediate() );
    }

    @Override
    protected void benchmarkTearDown() throws Exception
    {
        gbpTree.close();
        pageCache.close();
    }

    abstract long initialTreeSize();
}
