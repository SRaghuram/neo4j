/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.EntryRecord;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Function;

import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.Collections.emptyList;

/**
 * Recovers all the state required for operating the RAFT log and does some simple
 * verifications; e.g. checking for gaps, verifying headers.
 */
class RecoveryProtocol
{
    private static final SegmentHeader.Marshal headerMarshal = new SegmentHeader.Marshal();

    private final FileSystemAbstraction fileSystem;
    private final FileNames fileNames;
    private final Function<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector;
    private final LogProvider logProvider;
    private final Log log;
    private ReaderPool readerPool;

    RecoveryProtocol( FileSystemAbstraction fileSystem, FileNames fileNames, ReaderPool readerPool,
            Function<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector, LogProvider logProvider )
    {
        this.fileSystem = fileSystem;
        this.fileNames = fileNames;
        this.readerPool = readerPool;
        this.marshalSelector = marshalSelector;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    State run() throws IOException, DamagedLogStorageException, DisposedException
    {
        State state = new State();
        SortedMap<Long,File> files = fileNames.getAllFiles( fileSystem, log );

        if ( files.entrySet().isEmpty() )
        {
            state.segments = new Segments( fileSystem, fileNames, readerPool, emptyList(), marshalSelector, logProvider, -1 );
            state.segments.rotate( -1, -1, -1 );
            state.terms = new Terms( -1, -1 );
            return state;
        }

        List<SegmentFile> segmentFiles = new ArrayList<>();
        SegmentFile segment = null;

        long expectedSegmentNumber = files.firstKey();
        boolean mustRecoverLastHeader = false;
        boolean skip = true; // the first file is treated the same as a skip

        for ( Map.Entry<Long,File> entry : files.entrySet() )
        {
            long fileSegmentNumber = entry.getKey();
            File file = entry.getValue();
            SegmentHeader header;

            checkSegmentNumberSequence( fileSegmentNumber, expectedSegmentNumber );

            try
            {
                header = loadHeader( fileSystem, file );
                checkSegmentNumberMatches( header.segmentNumber(), fileSegmentNumber );
            }
            catch ( EndOfStreamException e )
            {
                if ( files.lastKey() != fileSegmentNumber )
                {
                    throw new DamagedLogStorageException( e, "Intermediate file with incomplete or no header found: %s", file );
                }
                else if ( files.size() == 1 )
                {
                    throw new DamagedLogStorageException( e, "Single file with incomplete or no header found: %s", file );
                }

                /* Last file header must be recovered by scanning next-to-last file and writing a new header based on that. */
                mustRecoverLastHeader = true;
                break;
            }

            segment = new SegmentFile( fileSystem, file, readerPool, fileSegmentNumber, marshalSelector.apply( header.formatVersion() ), logProvider, header );
            segmentFiles.add( segment );

            if ( segment.header().prevIndex() != segment.header().prevFileLastIndex() )
            {
                log.info( format( "Skipping from index %d to %d.", segment.header().prevFileLastIndex(),
                        segment.header().prevIndex() + 1 ) );
                skip = true;
            }

            if ( skip )
            {
                state.prevIndex = segment.header().prevIndex();
                state.prevTerm = segment.header().prevTerm();
                skip = false;
            }

            expectedSegmentNumber++;
        }

        assert segment != null;

        state.appendIndex = segment.header().prevIndex();
        state.terms = new Terms( segment.header().prevIndex(), segment.header().prevTerm() );

        try ( IOCursor<EntryRecord> cursor = segment.getCursor( segment.header().prevIndex() + 1 ) )
        {
            while ( cursor.next() )
            {
                EntryRecord entry = cursor.get();
                state.appendIndex = entry.logIndex();
                state.terms.append( state.appendIndex, entry.logEntry().term() );
            }
        }

        if ( mustRecoverLastHeader )
        {
            SegmentHeader header = new SegmentHeader( state.appendIndex, expectedSegmentNumber, state.appendIndex, state.terms.latest() );
            log.warn( "Recovering last file based on next-to-last file. " + header );

            File file = fileNames.getForSegment( expectedSegmentNumber );
            writeHeader( fileSystem, file, header );

            segment = new SegmentFile( fileSystem, file, readerPool, expectedSegmentNumber,
                    marshalSelector.apply( header.formatVersion() ), logProvider, header );
            segmentFiles.add( segment );
        }

        state.segments = new Segments( fileSystem, fileNames, readerPool, segmentFiles, marshalSelector, logProvider,
                segment.header().segmentNumber() );

        return state;
    }

    private static SegmentHeader loadHeader(
            FileSystemAbstraction fileSystem,
            File file ) throws IOException, EndOfStreamException
    {
        try ( StoreChannel channel = fileSystem.open( file, OpenMode.READ ) )
        {
            return headerMarshal.unmarshal( new ReadAheadChannel<>( channel, SegmentHeader.CURRENT_RECORD_OFFSET ) );
        }
    }

    private static void writeHeader(
            FileSystemAbstraction fileSystem,
            File file,
            SegmentHeader header ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.open( file, OpenMode.READ_WRITE ) )
        {
            channel.position( 0 );
            PhysicalFlushableChannel writer = new PhysicalFlushableChannel( channel, SegmentHeader.CURRENT_RECORD_OFFSET );
            headerMarshal.marshal( header, writer );
            writer.prepareForFlush().flush();
        }
    }

    private static void checkSegmentNumberSequence( long fileNameSegmentNumber, long expectedSegmentNumber ) throws DamagedLogStorageException
    {
        if ( fileNameSegmentNumber != expectedSegmentNumber )
        {
            throw new DamagedLogStorageException( "Segment numbers not strictly monotonic. Expected: %d but found: %d",
                    expectedSegmentNumber, fileNameSegmentNumber );
        }
    }

    private static void checkSegmentNumberMatches( long headerSegmentNumber, long fileNameSegmentNumber ) throws DamagedLogStorageException
    {
        if ( headerSegmentNumber != fileNameSegmentNumber )
        {
            throw new DamagedLogStorageException(
                    "File segment number does not match header. Expected: %d but found: %d", headerSegmentNumber, fileNameSegmentNumber );
        }
    }
}
