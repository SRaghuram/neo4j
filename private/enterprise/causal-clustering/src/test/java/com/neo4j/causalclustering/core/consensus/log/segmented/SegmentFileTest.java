/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.consensus.log.EntryRecord;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.consensus.ReplicatedString.valueOf;
import static com.neo4j.causalclustering.core.consensus.log.segmented.SegmentFile.create;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralTestDirectoryExtension
class SegmentFileTest
{
    @Inject
    private FileSystemAbstraction fsa;

    private final File baseDir = new File( "raft-log" );
    private final FileNames fileNames = new FileNames( baseDir );
    private final DummyRaftableContentSerializer contentMarshal = new DummyRaftableContentSerializer();
    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final SegmentHeader segmentHeader = new SegmentHeader( -1, 0, -1, -1 );

    // various constants used throughout tests
    private final RaftLogEntry entry1 = new RaftLogEntry( 30, valueOf( "contentA" ) );
    private final RaftLogEntry entry2 = new RaftLogEntry( 31, valueOf( "contentB" ) );
    private final RaftLogEntry entry3 = new RaftLogEntry( 32, valueOf( "contentC" ) );
    private final RaftLogEntry entry4 = new RaftLogEntry( 33, valueOf( "contentD" ) );
    private final int version = 0;

    private ReaderPool readerPool;

    @BeforeEach
    void before() throws IOException
    {
        readerPool = spy( new ReaderPool( 0, logProvider, fileNames, fsa, Clocks.fakeClock() ) );
        fsa.mkdirs( baseDir );
    }

    @Test
    void shouldReportCorrectInitialValues() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, version,
                contentMarshal, logProvider, segmentHeader, INSTANCE ) )
        {
            Assertions.assertEquals( 0, segment.header().segmentNumber() );

            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );
            Assertions.assertFalse( cursor.next() );

            cursor.close();
        }
    }

    @Test
    void shouldBeAbleToWriteAndRead() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.flush();

            // when
            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

            // then
            Assertions.assertTrue( cursor.next() );
            Assertions.assertEquals( entry1, cursor.get().logEntry() );

            cursor.close();
        }
    }

    @Test
    void shouldBeAbleToReadFromOffset() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.write( 2, entry3 );
            segment.write( 3, entry4 );
            segment.flush();

            // when
            IOCursor<EntryRecord> cursor = segment.getCursor( 2 );

            // then
            Assertions.assertTrue( cursor.next() );
            Assertions.assertEquals( entry3, cursor.get().logEntry() );

            cursor.close();
        }
    }

    @Test
    void shouldBeAbleToRepeatedlyReadWrittenValues() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.write( 2, entry3 );
            segment.flush();

            for ( int i = 0; i < 3; i++ )
            {
                // when
                IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

                // then
                Assertions.assertTrue( cursor.next() );
                Assertions.assertEquals( entry1, cursor.get().logEntry() );
                Assertions.assertTrue( cursor.next() );
                Assertions.assertEquals( entry2, cursor.get().logEntry() );
                Assertions.assertTrue( cursor.next() );
                Assertions.assertEquals( entry3, cursor.get().logEntry() );
                Assertions.assertFalse( cursor.next() );

                cursor.close();
            }
        }
    }

    @Test
    void shouldBeAbleToCloseOnlyAfterWriterIsClosed() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            // given
            Assertions.assertFalse( segment.tryClose() );

            // when
            segment.closeWriter();

            // then
            Assertions.assertTrue( segment.tryClose() );
        }
    }

    @Test
    void shouldCallDisposeHandlerAfterLastReaderIsClosed() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            // given
            IOCursor<EntryRecord> cursor0 = segment.getCursor( 0 );
            IOCursor<EntryRecord> cursor1 = segment.getCursor( 0 );

            // when
            segment.closeWriter();
            cursor0.close();

            // then
            Assertions.assertFalse( segment.tryClose() );

            // when
            cursor1.close();

            // then
            Assertions.assertTrue( segment.tryClose() );
        }
    }

    @Test
    void shouldHandleReaderPastEndCorrectly() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.flush();
            segment.closeWriter();

            IOCursor<EntryRecord> cursor = segment.getCursor( 3 );

            // then
            Assertions.assertFalse( cursor.next() );

            // when
            cursor.close();

            // then
            Assertions.assertTrue( segment.tryClose() );
        }
    }

    @Test
    void shouldHaveIdempotentCloseMethods() throws Exception
    {
        // given
        SegmentFile segment =
                create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal, logProvider,
                        segmentHeader, INSTANCE );
        IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

        // when
        segment.closeWriter();
        cursor.close();

        // then
        Assertions.assertTrue( segment.tryClose() );
        segment.close();
        Assertions.assertTrue( segment.tryClose() );
        segment.close();
    }

    @Test
    void shouldCatchDoubleCloseReaderErrors() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            // given
            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

            cursor.close();
            cursor.close();
            Assertions.fail( "Should have caught double close error" );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }

    @Test
    void shouldNotReturnReaderExperiencingErrorToPool() throws Exception
    {
        // given
        StoreChannel channel = mock( StoreChannel.class );
        Reader reader = mock( Reader.class );
        ReaderPool readerPool = mock( ReaderPool.class );

        when( channel.read( any( ByteBuffer.class ) ) ).thenThrow( new IOException() );
        when( reader.channel() ).thenReturn( channel );
        when( readerPool.acquire( anyLong(), anyLong() ) ).thenReturn( reader );

        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            // given
            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

            try
            {
                cursor.next();
                Assertions.fail();
            }
            catch ( IOException e )
            {
                // expected from mocking
            }

            // when
            cursor.close();

            // then
            verify( readerPool, never() ).release( reader );
            verify( reader ).close();
        }
    }

    @Test
    void shouldPruneReaderPoolOnClose() throws Exception
    {
        try ( SegmentFile segment = create( fsa, fileNames.getForSegment( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader, INSTANCE ) )
        {
            segment.write( 0, entry1 );
            segment.flush();
            segment.closeWriter();

            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );
            cursor.next();
            cursor.close();
        }

        verify( readerPool ).prune( 0 );
    }
}
