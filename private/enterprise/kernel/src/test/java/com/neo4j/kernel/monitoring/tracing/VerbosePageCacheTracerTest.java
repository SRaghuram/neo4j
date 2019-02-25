/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.monitoring.tracing;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.mockito.Mockito.mock;

class VerbosePageCacheTracerTest
{
    private AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private Log log = logProvider.getLog( getClass() );
    private FakeClock clock = Clocks.fakeClock();

    @Test
    void traceFileMap()
    {
        VerbosePageCacheTracer tracer = createTracer();
        tracer.mappedFile( new File( "mapFile" ) );
        logProvider.assertContainsMessageContaining( "Map file: 'mapFile'." );
    }

    @Test
    void traceUnmapFile()
    {
        VerbosePageCacheTracer tracer = createTracer();
        tracer.unmappedFile( new File( "unmapFile" ) );
        logProvider.assertContainsMessageContaining( "Unmap file: 'unmapFile'." );
    }

    @Test
    void traceSinglePageCacheFlush()
    {
        VerbosePageCacheTracer tracer = createTracer();
        try ( MajorFlushEvent majorFlushEvent = tracer.beginCacheFlush() )
        {
            FlushEventOpportunity flushEventOpportunity = majorFlushEvent.flushEventOpportunity();
            FlushEvent flushEvent = flushEventOpportunity.beginFlush( 1, 2, mock( PageSwapper.class) );
            flushEvent.addBytesWritten( 2 );
            flushEvent.addPagesFlushed( 7 );
            flushEvent.done();
        }
        logProvider.assertContainsMessageContaining( "Start whole page cache flush." );
        logProvider.assertLogStringContains( "Page cache flush completed. Flushed 2B in 7 pages. Flush took: 0ns. " +
                "Average speed: 2bytes/ns." );
    }

    @Test
    void evictionDoesNotInfluenceFlushNumbers()
    {
        VerbosePageCacheTracer tracer = createTracer();
        try ( MajorFlushEvent majorFlushEvent = tracer.beginCacheFlush() )
        {
            FlushEventOpportunity flushEventOpportunity = majorFlushEvent.flushEventOpportunity();
            FlushEvent flushEvent = flushEventOpportunity.beginFlush( 1, 2, mock( PageSwapper.class ) );
            clock.forward( 2, TimeUnit.MILLISECONDS );

            try ( EvictionRunEvent evictionRunEvent = tracer.beginPageEvictions( 5 ) )
            {
                try ( EvictionEvent evictionEvent = evictionRunEvent.beginEviction() )
                {
                    FlushEventOpportunity evictionEventOpportunity = evictionEvent.flushEventOpportunity();
                    FlushEvent evictionFlush = evictionEventOpportunity.beginFlush( 2, 3, mock( PageSwapper.class ) );
                    evictionFlush.addPagesFlushed( 10 );
                    evictionFlush.addPagesFlushed( 100 );
                }
            }
            flushEvent.addBytesWritten( 2 );
            flushEvent.addPagesFlushed( 7 );
            flushEvent.done();
        }
        logProvider.assertContainsMessageContaining( "Start whole page cache flush." );
        logProvider.assertLogStringContains( "Page cache flush completed. Flushed 2B in 7 pages. Flush took: 2ms. " +
                "Average speed: 0bytes/ns." );
    }

    @Test
    void traceFileFlush()
    {
        VerbosePageCacheTracer tracer = createTracer();
        PageSwapper swapper = mock( PageSwapper.class );
        Mockito.when( swapper.file() ).thenReturn( new File( "fileToFlush" ) );
        try ( MajorFlushEvent fileToFlush = tracer.beginFileFlush( swapper ) )
        {
            FlushEventOpportunity flushEventOpportunity = fileToFlush.flushEventOpportunity();
            FlushEvent flushEvent = flushEventOpportunity.beginFlush( 1, 2, swapper );
            flushEvent.addPagesFlushed( 100 );
            flushEvent.addBytesWritten( ByteUnit.ONE_MEBI_BYTE );
            flushEvent.done();
            clock.forward( 1, TimeUnit.SECONDS );
            FlushEvent flushEvent2 = flushEventOpportunity.beginFlush( 1, 2, swapper );
            flushEvent2.addPagesFlushed( 10 );
            flushEvent2.addBytesWritten( ByteUnit.ONE_MEBI_BYTE );
            flushEvent2.done();
        }
        logProvider.assertContainsMessageContaining( "Flushing file: 'fileToFlush'." );
        logProvider.assertLogStringContains( "'fileToFlush' flush completed. Flushed 2.000MiB in 110 pages. Flush took: 1s. Average speed: 2.000MiB/s." );
    }

    private VerbosePageCacheTracer createTracer()
    {
        return new VerbosePageCacheTracer( log, clock );
    }
}
