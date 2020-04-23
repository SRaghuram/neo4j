/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Function;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

import static java.lang.String.format;

/**
 * Keeps track of all the segments that the RAFT log consists of.
 */
class Segments implements AutoCloseable
{
    private final OpenEndRangeMap<Long/*minIndex*/,SegmentFile> rangeMap = new OpenEndRangeMap<>();
    private final List<SegmentFile> allSegments;
    private final Log log;

    private FileSystemAbstraction fileSystem;
    private final FileNames fileNames;
    private final Function<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector;
    private final LogProvider logProvider;
    private long currentVersion;
    private final ReaderPool readerPool;
    private final MemoryTracker memoryTracker;

    Segments( FileSystemAbstraction fileSystem, FileNames fileNames, ReaderPool readerPool, List<SegmentFile> allSegments,
            Function<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector, LogProvider logProvider, long currentVersion, MemoryTracker memoryTracker )
    {
        this.fileSystem = fileSystem;
        this.fileNames = fileNames;
        this.allSegments = new ArrayList<>( allSegments );
        this.marshalSelector = marshalSelector;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.currentVersion = currentVersion;
        this.readerPool = readerPool;
        this.memoryTracker = memoryTracker;

        populateRangeMap();
    }

    private void populateRangeMap()
    {
        for ( SegmentFile segment : allSegments )
        {
            rangeMap.replaceFrom( segment.header().prevIndex() + 1, segment );
        }
    }

    /*
     * Simple chart demonstrating valid and invalid value combinations for the following three calls. All three
     * result in the same action, but they demand different invariants. Whether we choose to fail hard when they are
     * invalidated or to simply log a warning, we should still make some sort of check against them.
     *
     * Valid truncate: prevFileLast = 100, prevIndex = 80
     * Invalid truncate: prevFileLast = 100, prevIndex = 101
     *
     * Valid rotate: prevFileLast = 100, prevIndex = 100
     * Invalid rotate: prevFileLast = 100, prevIndex = 80
     * Invalid rotate: prevFileLast = 100, prevIndex = 101
     *
     * Valid skip: prevFileLast = 100, prevIndex = 101
     * Invalid skip: prevFileLast = 100, prevIndex = 80
     */
    synchronized SegmentFile truncate( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        if ( prevFileLastIndex < prevIndex )
        {
            throw new IllegalArgumentException( format( "Cannot truncate at index %d which is after current " +
                                                        "append index %d", prevIndex, prevFileLastIndex ) );
        }
        if ( prevFileLastIndex == prevIndex )
        {
            log.warn( format( "Truncating at current log append index %d", prevIndex ) );
        }
        return createNext( prevFileLastIndex, prevIndex, prevTerm );
    }

    synchronized SegmentFile rotate( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        if ( prevFileLastIndex != prevIndex )
        {
            throw new IllegalArgumentException( format( "Cannot rotate file and have append index go from %d " +
                                                        "to %d. Going backwards is a truncation operation, going " +
                                                        "forwards is a skip operation.",
                    prevFileLastIndex, prevIndex ) );
        }
        return createNext( prevFileLastIndex, prevIndex, prevTerm );
    }

    synchronized SegmentFile skip( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        if ( prevFileLastIndex > prevIndex )
        {
            throw new IllegalArgumentException( format( "Cannot skip from index %d backwards to index %d",
                    prevFileLastIndex, prevIndex ) );
        }
        if ( prevFileLastIndex == prevIndex )
        {
            log.warn( format( "Skipping at current log append index %d", prevIndex ) );
        }
        return createNext( prevFileLastIndex, prevIndex, prevTerm );
    }

    private synchronized SegmentFile createNext( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        currentVersion++;
        SegmentHeader header = new SegmentHeader( prevFileLastIndex, currentVersion, prevIndex, prevTerm );

        File file = fileNames.getForSegment( currentVersion );
        ChannelMarshal<ReplicatedContent> contentMarshal = marshalSelector.apply( header.formatVersion() );
        SegmentFile segment = SegmentFile.create( fileSystem, file, readerPool, currentVersion, contentMarshal, logProvider, header, memoryTracker );
        // TODO: Force base directory... probably not possible using fsa.
        segment.flush();

        allSegments.add( segment );
        rangeMap.replaceFrom( prevIndex + 1, segment );

        return segment;
    }

    synchronized OpenEndRangeMap.ValueRange<Long,SegmentFile> getForIndex( long logIndex )
    {
        return rangeMap.lookup( logIndex );
    }

    synchronized SegmentFile last()
    {
        return rangeMap.last();
    }

    public synchronized SegmentFile prune( long pruneIndex )
    {
        Iterator<SegmentFile> itr = allSegments.iterator();
        SegmentFile notDisposed = itr.next(); // we should always leave at least one segment
        int firstRemaining = 0;

        while ( itr.hasNext() )
        {
            SegmentFile current = itr.next();
            if ( current.header().prevFileLastIndex() > pruneIndex )
            {
                break;
            }

            if ( !notDisposed.tryClose() )
            {
                break;
            }

            log.info( "Pruning %s", notDisposed );
            if ( !notDisposed.delete() )
            {
                log.error( "Failed to delete %s", notDisposed );
                break;
            }

            // TODO: Sync the parent directory. Also consider handling fs operations under its own lock.

            firstRemaining++;
            notDisposed = current;
        }

        rangeMap.remove( notDisposed.header().prevIndex() + 1 );
        allSegments.subList( 0, firstRemaining ).clear();

        return notDisposed;
    }

    synchronized void visit( Visitor<SegmentFile,RuntimeException> visitor )
    {
        ListIterator<SegmentFile> itr = allSegments.listIterator();

        boolean terminate = false;
        while ( itr.hasNext() && !terminate )
        {
            terminate = visitor.visit( itr.next() );
        }
    }

    synchronized void visitBackwards( Visitor<SegmentFile,RuntimeException> visitor )
    {
        ListIterator<SegmentFile> itr = allSegments.listIterator( allSegments.size() );

        boolean terminate = false;
        while ( itr.hasPrevious() && !terminate )
        {
            terminate = visitor.visit( itr.previous() );
        }
    }

    @Override
    public synchronized void close()
    {
        RuntimeException error = null;
        for ( SegmentFile segment : allSegments )
        {
            try
            {
                segment.close();
            }
            catch ( RuntimeException ex )
            {
                if ( error == null )
                {
                    error = ex;
                }
                else
                {
                    error.addSuppressed( ex );
                }
            }
        }

        if ( error != null )
        {
            throw error;
        }
    }
}
