/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.monitoring.tracing;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.helpers.TimeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.logging.Log;
import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class VerbosePageCacheTracer extends DefaultPageCacheTracer
{
    private final Log log;
    private final SystemNanoClock clock;
    private final AtomicLong flushedPages = new AtomicLong();
    private final AtomicLong mergedPages = new AtomicLong();
    private final AtomicLong flushBytesWritten = new AtomicLong();
    private final Duration speedReportingThresholdSeconds;

    VerbosePageCacheTracer( Log log, SystemNanoClock clock, Config config )
    {
        this.log = log;
        this.clock = clock;
        this.speedReportingThresholdSeconds = config.get( GraphDatabaseInternalSettings.page_cache_tracer_speed_reporting_threshold );
    }

    @Override
    public void mappedFile( Path path )
    {
        log.info( format( "Map file: '%s'.", path.getFileName() ) );
        super.mappedFile( path );
    }

    @Override
    public void unmappedFile( Path path )
    {
        log.info( format( "Unmap file: '%s'.", path.getFileName() ) );
        super.unmappedFile( path );
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        log.info( "Start whole page cache flush." );
        return new PageCacheMajorFlushEvent( flushedPages.get(), flushBytesWritten.get(), mergedPages.get(), clock.startStopWatch() );
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        String fileName = swapper.path().getFileName().toString();
        log.info( format( "Flushing file: '%s'.", fileName ) );
        return new FileFlushEvent( fileName, flushedPages.get(), flushBytesWritten.get(), mergedPages.get(), clock.startStopWatch() );
    }

    private static String nanosToString( long nanos )
    {
        return format( "%s (%d ns)", TimeUtil.nanosToString( nanos ), nanos );
    }

    private static String flushSpeed( long bytesWrittenInTotal, long flushTimeNanos )
    {
        String bytesPerNano = bytesInNanoSeconds( bytesWrittenInTotal, flushTimeNanos );
        long seconds = TimeUnit.NANOSECONDS.toSeconds( flushTimeNanos );
        if ( seconds > 0 )
        {
            return bytesToString( bytesWrittenInTotal / seconds ) + "/s" + " (" + bytesPerNano + ")";
        }
        else
        {
            return bytesPerNano;
        }
    }

    private static String bytesInNanoSeconds( long bytesWrittenInTotal, long flushTimeNanos )
    {
        long bytesInNanoSecond = flushTimeNanos > 0 ? (bytesWrittenInTotal / flushTimeNanos) : bytesWrittenInTotal;
        return bytesInNanoSecond + "bytes/ns";
    }

    private static String bytesToString( long bytes )
    {
        return format( "%s (%d bytes)", ByteUnit.bytesToString( bytes ), bytes );
    }

    private final FlushEvent flushEvent = new FlushEvent()
    {
        @Override
        public void addBytesWritten( long bytes )
        {
            bytesWritten.add( bytes );
            flushBytesWritten.getAndAdd( bytes );
        }

        @Override
        public void done()
        {
            flushes.increment();
        }

        @Override
        public void done( IOException exception )
        {
            done();
        }

        @Override
        public void addPagesFlushed( int pageCount )
        {
            flushedPages.getAndAdd( pageCount );
        }

        @Override
        public void addPagesMerged( int pagesMerged )
        {
            mergedPages.getAndAdd( pagesMerged );
        }
    };

    private class FileFlushEvent implements MajorFlushEvent
    {
        private final long mergesOnStart;
        private final Stopwatch startTime;
        private final String fileName;
        private final long flushesOnStart;
        private final long bytesWrittenOnStart;

        FileFlushEvent( String fileName, long flushesOnStart, long bytesWrittenOnStart, long mergesOnStart, Stopwatch startTime )
        {
            this.fileName = fileName;
            this.flushesOnStart = flushesOnStart;
            this.bytesWrittenOnStart = bytesWrittenOnStart;
            this.mergesOnStart = mergesOnStart;
            this.startTime = startTime;
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return new VerboseFlushOpportunity( fileName, startTime, bytesWrittenOnStart );
        }

        @Override
        public void close()
        {
            long fileFlushNanos = startTime.elapsed( NANOSECONDS );
            long bytesWrittenInTotal = flushBytesWritten.get() - bytesWrittenOnStart;
            long flushedPagesInTotal = flushedPages.get() - flushesOnStart;
            long mergedPagesInTotal = mergedPages.get() - mergesOnStart;
            log.info( "'%s' flush completed. Flushed %s in %d pages, %d pages merged. Flush took: %s. Average speed: %s.",
                    fileName,
                    bytesToString( bytesWrittenInTotal ), flushedPagesInTotal, mergedPagesInTotal,
                    nanosToString( fileFlushNanos ), flushSpeed( bytesWrittenInTotal, fileFlushNanos ) );
        }
    }

    private class PageCacheMajorFlushEvent implements MajorFlushEvent
    {
        private final long flushesOnStart;
        private final long bytesWrittenOnStart;
        private final long mergesOnStart;
        private final Stopwatch startTime;

        PageCacheMajorFlushEvent( long flushesOnStart, long bytesWrittenOnStart, long mergesOnStart, Stopwatch startTime )
        {
            this.flushesOnStart = flushesOnStart;
            this.bytesWrittenOnStart = bytesWrittenOnStart;
            this.mergesOnStart = mergesOnStart;
            this.startTime = startTime;
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return new VerboseFlushOpportunity( "Page Cache", startTime, bytesWrittenOnStart );
        }

        @Override
        public void close()
        {
            long pageCacheFlushNanos = startTime.elapsed( NANOSECONDS );
            long bytesWrittenInTotal = flushBytesWritten.get() - bytesWrittenOnStart;
            long flushedPagesInTotal = flushedPages.get() - flushesOnStart;
            long mergedPagesInTotal = mergedPages.get() - mergesOnStart;
            log.info( "Page cache flush completed. Flushed %s in %d pages, %d pages merged. Flush took: %s. Average speed: %s.",
                    bytesToString( bytesWrittenInTotal ), flushedPagesInTotal, mergedPagesInTotal,
                    nanosToString(pageCacheFlushNanos),
                    flushSpeed( bytesWrittenInTotal, pageCacheFlushNanos ) );
        }
    }

    private class VerboseFlushOpportunity implements FlushEventOpportunity
    {
        private final String fileName;
        private Stopwatch lastReportingTime;
        private long lastReportedBytesWritten;

        VerboseFlushOpportunity( String fileName, Stopwatch startTime, long bytesWrittenOnStart )
        {
            this.fileName = fileName;
            this.lastReportingTime = startTime;
            this.lastReportedBytesWritten = bytesWrittenOnStart;
        }

        @Override
        public FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper, int pagesToFlush, int mergedPages )
        {
            if ( lastReportingTime.hasTimedOut( speedReportingThresholdSeconds ) )
            {
                long writtenBytes = flushBytesWritten.get();
                log.info( format("'%s' flushing speed: %s.", fileName,
                        flushSpeed( writtenBytes - lastReportedBytesWritten, lastReportingTime.elapsed( NANOSECONDS ) ) ) );
                lastReportingTime = clock.startStopWatch();
                lastReportedBytesWritten = writtenBytes;
            }
            log.info( "Flushing %d pages. %d are merged.", pagesToFlush, mergedPages );
            return flushEvent;
        }

        @Override
        public void startFlush( int[][] translationTable )
        {
            log.info( "'%s' translation table size: %d.", fileName, translationTable.length );
        }

        @Override
        public ChunkEvent startChunk( int[] chunk )
        {
            return new VerboseChunkEvent( fileName, clock.startStopWatch() );
        }
    }

    private class VerboseChunkEvent extends FlushEventOpportunity.ChunkEvent
    {
        private final String fileName;
        private final Stopwatch startTime;

        VerboseChunkEvent( String fileName, Stopwatch startTime )
        {
            this.fileName = fileName;
            this.startTime = startTime;
        }

        @Override
        public void chunkFlushed( long notModifiedPages, long flushPerChunk, long buffersPerChunk, long mergesPerChunk )
        {
            log.info( "'%s' chunk flushed. Not modified pages: %d, flushes: %d, used buffers: %d, merge: %d in %s.", fileName,
                    notModifiedPages, flushPerChunk, buffersPerChunk, mergesPerChunk, nanosToString( startTime.elapsed( NANOSECONDS ) ) );
        }
    }
}
