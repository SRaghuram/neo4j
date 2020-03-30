/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.pageCache;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.benchmarks.Throttler;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import org.openjdk.jmh.infra.ThreadParams;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.MemoryPools;
import org.neo4j.time.Clocks;

import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;
import static org.neo4j.io.ByteUnit.KibiByte;

public abstract class AbstractPageCacheBenchmarkV2 extends BaseDatabaseBenchmark
{
    private static final ExecutorService executor;
    private static final int TARGET_INTERFERING_OPSSEC;
    private static final long FILE_SIZE;
    private static final File STORE_FILE;
    private static final ByteBuffer buf;

    static
    {
        executor = Executors.newCachedThreadPool( r ->
                                                  {
                                                      Thread th = new Thread( r );
                                                      th.setDaemon( true );
                                                      return th;
                                                  } );
        TARGET_INTERFERING_OPSSEC = 1_000_000;
        FILE_SIZE = ByteUnit.mebiBytes( 100 );
        try
        {
            STORE_FILE = File.createTempFile( "neostore.nodestore.db", "tmp" ).getCanonicalFile();
            STORE_FILE.deleteOnExit();
        }
        catch ( IOException e )
        {
            throw new AssertionError( e );
        }
        buf = ByteBuffers.allocate( 8, KibiByte );
        // NOTE: can not use SplittableRandom as it does not have nextBytes()
        ThreadLocalRandom.current().nextBytes( buf.array() );
    }

    private PageCache pageCache;
    PagedFile pagedFile;

    private Future<?> interferenceFuture;
    private volatile boolean interferenceStopped;

    @Override
    public String benchmarkGroup()
    {
        return "Page Cache";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    protected long getFileSize()
    {
        return FILE_SIZE;
    }

    @Override
    protected StartDatabaseInstruction afterDataGeneration()
    {
        try
        {
            setUpForPageCache();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error setting up page cache", e );
        }

        return StartDatabaseInstruction.DO_NOT_START_DB;
    }

    private void setUpForPageCache() throws IOException
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        fs.mkdirs( STORE_FILE.getParentFile() );
        try ( StoreChannel storeChannel = fs.write( STORE_FILE ) )
        {
            long fileSize = 0;
            while ( fileSize < getFileSize() )
            {
                fileSize += storeChannel.write( buf );
                buf.clear();
            }
            storeChannel.truncate( getFileSize() );
        }

        long pageCacheMemory = (long) (getFileSize() * getPercentageCached());
        Config config = Config.defaults( GraphDatabaseSettings.pagecache_memory, pageCacheMemory + "" );
        PageCacheTracer tracer = new DefaultPageCacheTracer();
        Log log = NullLog.getInstance();
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                tracer, log,
                EmptyVersionContextSupplier.EMPTY,
                JobSchedulerFactory.createInitialisedScheduler(),
                Clocks.nanoClock(),
                new MemoryPools() );
        pageCache = factory.getOrCreatePageCache();
        pagedFile = pageCache.map( STORE_FILE, (int) ByteUnit.kibiBytes( 8 ) );
        if ( getPercentageCached() > 0.49 )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK, PageCursorTracer.NULL ) )
            {
                //noinspection StatementWithEmptyBody
                while ( cursor.next() )
                {
                    // do nothing
                }
            }
        }
    }

    @Override
    protected void benchmarkTearDown() throws IOException
    {
        pagedFile.close();
        pageCache.close();
    }

    protected abstract double getPercentageCached();

    public abstract static class CursorState
    {
        private ValueGeneratorFun<Long> ids;
        PageCursor pageCursor;

        public void setUp( ThreadParams threadParams, AbstractPageCacheBenchmarkV2 benchmarkState ) throws IOException
        {
            PagedFile pagedFile = benchmarkState.pagedFile;
            pageCursor = pagedFile.io( 1, getPageFlags(), PageCursorTracer.NULL );

            int threadCount = threadParams.getThreadCount();
            int threadIndex = threadParams.getThreadIndex();
            ids = nonContendingStridingFor(
                    LNG,
                    threadCount,
                    threadIndex,
                    pagedFile.getLastPageId() ).create();
        }

        public long id( SplittableRandom rng )
        {
            return ids.next( rng );
        }

        public void tearDown() throws IOException
        {
            pageCursor.close();
        }

        public abstract int getPageFlags();
    }

    void startInterference( int flags )
    {
        interferenceStopped = false;
        interferenceFuture = executor.submit( () ->
                                              {
                                                  Throttler throttler = new Throttler( TARGET_INTERFERING_OPSSEC );
                                                  try ( PageCursor cursor = pagedFile.io( 0, flags, PageCursorTracer.NULL ) )
                                                  {
                                                      long pages = pagedFile.fileSize() / pagedFile.pageSize();
                                                      long counter = 0;
                                                      do
                                                      {
                                                          throttler.waitForNext();
                                                          cursor.next( counter % pages );
                                                          counter++;
                                                      }
                                                      while ( !interferenceStopped );
                                                  }
                                                  catch ( IOException e )
                                                  {
                                                      e.printStackTrace();
                                                  }
                                              } );
    }

    void stopInterference() throws InterruptedException, ExecutionException
    {
        interferenceStopped = true;
        interferenceFuture.get();
    }
}
