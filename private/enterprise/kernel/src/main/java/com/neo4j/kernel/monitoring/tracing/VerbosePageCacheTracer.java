/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.monitoring.tracing;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.util.FeatureToggles.getInteger;

public class VerbosePageCacheTracer extends DefaultPageCacheTracer
{
    private static final boolean USE_RAW_REPORTING_UNITS =
            flag( VerbosePageCacheTracer.class, "reportInRawUnits", false );
    private static final int SPEED_REPORTING_TIME_THRESHOLD = getInteger( VerbosePageCacheTracer.class,
            "speedReportingThresholdSeconds", 10 );

    private final Log log;
    private final SystemNanoClock clock;
    private final AtomicLong flushedPages = new AtomicLong();
    private final AtomicLong flushBytesWritten = new AtomicLong();

    VerbosePageCacheTracer( Log log, SystemNanoClock clock )
    {
        this.log = log;
        this.clock = clock;
    }

    @Override
    public void mappedFile( File file )
    {
        log.info( format( "Map file: '%s'.", file.getName() ) );
        super.mappedFile( file );
    }

    @Override
    public void unmappedFile( File file )
    {
        log.info( format( "Unmap file: '%s'.", file.getName() ) );
        super.unmappedFile( file );
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        log.info( "Start whole page cache flush." );
        return new PageCacheMajorFlushEvent( flushedPages.get(), flushBytesWritten.get(), clock.startStopWatch() );
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        String fileName = swapper.file().getName();
        log.info( format( "Flushing file: '%s'.", fileName ) );
        return new FileFlushEvent( fileName, flushedPages.get(), flushBytesWritten.get(), clock.startStopWatch() );
    }

    private static String nanosToString( long nanos )
    {
        if ( USE_RAW_REPORTING_UNITS )
        {
            return nanos + "ns";
        }
        return TimeUtil.nanosToString( nanos );
    }

    private static String flushSpeed( long bytesWrittenInTotal, long flushTimeNanos )
    {
        if ( USE_RAW_REPORTING_UNITS )
        {
            return bytesInNanoSeconds( bytesWrittenInTotal, flushTimeNanos );
        }
        long seconds = TimeUnit.NANOSECONDS.toSeconds( flushTimeNanos );
        if ( seconds > 0 )
        {
            return bytesToString( bytesWrittenInTotal / seconds ) + "/s";
        }
        else
        {
            return bytesInNanoSeconds( bytesWrittenInTotal, flushTimeNanos );
        }
    }

    private static String bytesInNanoSeconds( long bytesWrittenInTotal, long flushTimeNanos )
    {
        long bytesInNanoSecond = flushTimeNanos > 0 ? (bytesWrittenInTotal / flushTimeNanos) : bytesWrittenInTotal;
        return bytesInNanoSecond + "bytes/ns";
    }

    private static String bytesToString( long bytes )
    {
        if ( USE_RAW_REPORTING_UNITS )
        {
            return bytes + "bytes";
        }
        return ByteUnit.bytesToString( bytes );
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
    };

    private class FileFlushEvent implements MajorFlushEvent
    {
        private final Stopwatch startTime;
        private final String fileName;
        private final long flushesOnStart;
        private final long bytesWrittenOnStart;

        FileFlushEvent( String fileName, long flushesOnStart, long bytesWrittenOnStart, Stopwatch startTime )
        {
            this.fileName = fileName;
            this.flushesOnStart = flushesOnStart;
            this.bytesWrittenOnStart = bytesWrittenOnStart;
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
            log.info( "'%s' flush completed. Flushed %s in %d pages. Flush took: %s. Average speed: %s.",
                    fileName,
                    bytesToString( bytesWrittenInTotal ), flushedPagesInTotal,
                    nanosToString( fileFlushNanos ), flushSpeed( bytesWrittenInTotal, fileFlushNanos ) );
        }
    }

    private class PageCacheMajorFlushEvent implements MajorFlushEvent
    {
        private final long flushesOnStart;
        private final long bytesWrittenOnStart;
        private final Stopwatch startTime;

        PageCacheMajorFlushEvent( long flushesOnStart, long bytesWrittenOnStart, Stopwatch startTime )
        {
            this.flushesOnStart = flushesOnStart;
            this.bytesWrittenOnStart = bytesWrittenOnStart;
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
            log.info( "Page cache flush completed. Flushed %s in %d pages. Flush took: %s. Average speed: %s.",
                    bytesToString( bytesWrittenInTotal ), flushedPagesInTotal,
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
            if ( lastReportingTime.hasTimedOut( SPEED_REPORTING_TIME_THRESHOLD, TimeUnit.SECONDS ) )
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
    }
}
