/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.EntryRecord;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class SegmentsTest
{
    private final FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
    private final File baseDirectory = new File( "." );
    private final FileNames fileNames = new FileNames( baseDirectory );
    @SuppressWarnings( "unchecked" )
    private final ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
    private final Function<Integer,ChannelMarshal<ReplicatedContent>> contentMarshals = ignored -> contentMarshal;
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final SegmentHeader header = mock( SegmentHeader.class );
    private final ReaderPool readerPool = new ReaderPool( 0, getInstance(), fileNames, fsa,
            Clocks.fakeClock() );

    private final SegmentFile fileA = spy( new SegmentFile( fsa, fileNames.getForSegment( 0 ), readerPool, 0,
            contentMarshal, logProvider, header, INSTANCE ) );
    private final SegmentFile fileB = spy( new SegmentFile( fsa, fileNames.getForSegment( 1 ), readerPool, 1,
            contentMarshal, logProvider, header, INSTANCE ) );

    private final List<SegmentFile> segmentFiles = asList( fileA, fileB );

    @BeforeEach
    void before()
    {
        when( fsa.deleteFile( any() ) ).thenReturn( true );
    }

    @Test
    void shouldCreateNext() throws Exception
    {
        // Given
        try ( Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshals,
                logProvider, -1, INSTANCE ) )
        {
            // When
            segments.rotate( 10, 10, 12 );
            segments.last().closeWriter();
            SegmentFile last = segments.last();

            // Then
            Assertions.assertEquals( 10, last.header().prevFileLastIndex() );
            Assertions.assertEquals( 10, last.header().prevIndex() );
            Assertions.assertEquals( 12, last.header().prevTerm() );
        }
    }

    @Test
    void shouldDeleteOnPrune() throws Exception
    {
        verifyNoInteractions( fsa );
        // Given
        try ( Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshals,
                logProvider, -1, INSTANCE ) )
        {
            // this is version 0 and will be deleted on prune later
            SegmentFile toPrune = segments.rotate( -1, -1, -1 );
            segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
            segments.rotate( 10, 10, 2 );
            segments.last().closeWriter(); // ditto
            segments.rotate( 20, 20, 2 );

            // When
            segments.prune( 11 );

            verify( fsa, times( segmentFiles.size() ) ).deleteFile( fileNames.getForSegment( toPrune.header().segmentNumber() ) );
        }
    }

    @Test
    void shouldNeverDeleteOnTruncate() throws Exception
    {
        // Given
        try ( Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshals,
                logProvider, -1, INSTANCE ) )
        {
            segments.rotate( -1, -1, -1 );
            segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
            segments.rotate( 10, 10, 2 ); // we will truncate this whole file away
            segments.last().closeWriter();

            // When
            segments.truncate( 20, 9, 4 );

            // Then
            verify( fsa, never() ).deleteFile( any() );
        }
    }

    @Test
    void shouldDeleteTruncatedFilesOnPrune() throws Exception
    {
        // Given
        try ( Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshals,
                logProvider, -1, INSTANCE ) )
        {
            SegmentFile toBePruned = segments.rotate( -1, -1, -1 );
            segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
            // we will truncate this whole file away
            SegmentFile toBeTruncated = segments.rotate( 10, 10, 2 );
            segments.last().closeWriter();

            // When
            // We truncate a whole file
            segments.truncate( 20, 9, 4 );
            // And we prune all files before that file
            segments.prune( 10 );

            // Then
            // the truncate file is part of the deletes that happen while pruning
            verify( fsa, times( segmentFiles.size() ) ).deleteFile(
                    fileNames.getForSegment( toBePruned.header().segmentNumber() ) );
        }
    }

    @Test
    void shouldCloseTheSegments()
    {
        // Given
        Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshals, logProvider, -1, INSTANCE );

        // When
        segments.close();

        // Then
        for ( SegmentFile file : segmentFiles )
        {
            verify( file ).close();
        }
    }

    @Test
    void shouldNotSwallowExceptionOnClose()
    {
        // Given
        doThrow( new RuntimeException() ).when( fileA ).close();
        doThrow( new RuntimeException() ).when( fileB ).close();

        Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshals, logProvider, -1, INSTANCE );

        // When
        try
        {
            segments.close();
            Assertions.fail( "should have thrown" );
        }
        catch ( RuntimeException ex )
        {
            // Then
            Throwable[] suppressed = ex.getSuppressed();
            Assertions.assertEquals( 1, suppressed.length );
            Assertions.assertTrue( suppressed[0] instanceof RuntimeException );
        }
    }

    @Test
    void shouldAllowOutOfBoundsPruneIndex() throws Exception
    {
        //Given a prune index of n, if the smallest value for a segment file is n+c, the pruning should not remove
        // any files and not result in a failure.
        Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshals, logProvider, -1, INSTANCE );

        segments.rotate( -1, -1, -1 );
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
        segments.rotate( 10, 10, 2 ); // we will truncate this whole file away
        segments.last().closeWriter();

        segments.prune( 11 );

        segments.rotate( 20, 20, 3 ); // we will truncate this whole file away
        segments.last().closeWriter();

        //when
        SegmentFile oldestNotDisposed = segments.prune( -1 );

        //then
        SegmentHeader header = oldestNotDisposed.header();
        Assertions.assertEquals( 10, header.prevFileLastIndex() );
        Assertions.assertEquals( 10, header.prevIndex() );
        Assertions.assertEquals( 2, header.prevTerm() );
    }

    @Test
    void attemptsPruningUntilOpenFileIsFound() throws Exception
    {
        /**
         * prune stops attempting to prune files after finding one that is open.
         */

        // Given
        Segments segments = new Segments( fsa, fileNames, readerPool, Collections.emptyList(), contentMarshals, logProvider, -1, INSTANCE );

        /*
        create 0
        create 1
        create 2
        create 3

        closeWriter on all
        create reader on 1
        prune on 3

        only 0 should be deleted
         */

        segments.rotate( -1, -1, -1 );
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called

        segments.rotate( 10, 10, 2 ); // we will truncate this whole file away
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
        IOCursor<EntryRecord> reader = segments.last().getCursor( 11 );

        segments.rotate( 20, 20, 3 ); // we will truncate this whole file away
        segments.last().closeWriter();

        segments.rotate( 30, 30, 4 ); // we will truncate this whole file away
        segments.last().closeWriter();

        segments.prune( 31 );

        //when
        OpenEndRangeMap.ValueRange<Long,SegmentFile> shouldBePruned = segments.getForIndex( 5 );
        OpenEndRangeMap.ValueRange<Long,SegmentFile> shouldNotBePruned = segments.getForIndex( 15 );
        OpenEndRangeMap.ValueRange<Long,SegmentFile> shouldAlsoNotBePruned = segments.getForIndex( 25 );

        //then
        Assertions.assertFalse( shouldBePruned.value().isPresent() );
        Assertions.assertTrue( shouldNotBePruned.value().isPresent() );
        Assertions.assertTrue( shouldAlsoNotBePruned.value().isPresent() );

        //when
        reader.close();
        segments.prune( 31 );

        shouldBePruned = segments.getForIndex( 5 );
        shouldNotBePruned = segments.getForIndex( 15 );
        shouldAlsoNotBePruned = segments.getForIndex( 25 );

        //then
        Assertions.assertFalse( shouldBePruned.value().isPresent() );
        Assertions.assertFalse( shouldNotBePruned.value().isPresent() );
        Assertions.assertFalse( shouldAlsoNotBePruned.value().isPresent() );
    }
}
