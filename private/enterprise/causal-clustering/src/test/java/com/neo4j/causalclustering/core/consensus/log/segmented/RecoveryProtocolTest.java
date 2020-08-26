/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.time.Clocks;

import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralTestDirectoryExtension
class RecoveryProtocolTest
{
    @Inject
    private FileSystemAbstraction fsa;

    private Function<Integer,ChannelMarshal<ReplicatedContent>> contentMarshal = ignored -> new DummyRaftableContentSerializer();
    private final Path root = Path.of( "root" );
    private final FileNames fileNames = new FileNames( root );
    private final SegmentHeader.Marshal headerMarshal = new SegmentHeader.Marshal();
    private ReaderPool readerPool;

    @BeforeEach
    void setup() throws IOException
    {
        fsa.mkdirs( root );
        readerPool = new ReaderPool( 0, getInstance(), fileNames, fsa, Clocks.fakeClock() );
    }

    @Test
    void shouldReturnEmptyStateOnEmptyDirectory() throws Exception
    {
        // given
        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        // when
        State state = protocol.run();

        // then
        Assertions.assertEquals( -1, state.appendIndex );
        Assertions.assertEquals( -1, state.terms.latest() );
        Assertions.assertEquals( -1, state.prevIndex );
        Assertions.assertEquals( -1, state.prevTerm );
        Assertions.assertEquals( 0, state.segments.last().header().segmentNumber() );
    }

    @Test
    void shouldFailIfThereAreGapsInVersionNumberSequence() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 5, 2, 2, 5, 0 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        try
        {
            // when
            protocol.run();
            Assertions.fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    void shouldFailIfTheVersionNumberInTheHeaderAndFileNameDiffer() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 1, -1, -1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        try
        {
            // when
            protocol.run();
            Assertions.fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    void shouldFailIfANonLastFileIsMissingHeader() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createEmptyLogFile( fsa, 1 );
        createLogFile( fsa, -1, 2, 2, -1, -1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        try
        {
            // when
            protocol.run();
            Assertions.fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    void shouldRecoverEvenIfLastHeaderIsMissing() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createEmptyLogFile( fsa, 1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        // when
        protocol.run();

        // then
        Assertions.assertNotEquals( 0, fsa.getFileSize( fileNames.getForSegment( 1 ) ) );
    }

    @Test
    void shouldRecoverAndBeAbleToRotate() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10, 0 );
        createLogFile( fsa, 20, 2, 2, 20, 1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.rotate( 20, 20, 1 );

        // then
        Assertions.assertEquals( 20, newFile.header().prevFileLastIndex() );
        Assertions.assertEquals( 3, newFile.header().segmentNumber() );
        Assertions.assertEquals( 20, newFile.header().prevIndex() );
        Assertions.assertEquals( 1, newFile.header().prevTerm() );
    }

    @Test
    void shouldRecoverAndBeAbleToTruncate() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10, 0 );
        createLogFile( fsa, 20, 2, 2, 20, 1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.truncate( 20, 15, 0 );

        // then
        Assertions.assertEquals( 20, newFile.header().prevFileLastIndex() );
        Assertions.assertEquals( 3, newFile.header().segmentNumber() );
        Assertions.assertEquals( 15, newFile.header().prevIndex() );
        Assertions.assertEquals( 0, newFile.header().prevTerm() );
    }

    @Test
    void shouldRecoverAndBeAbleToSkip() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10, 0 );
        createLogFile( fsa, 20, 2, 2, 20, 1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.skip( 20, 40, 2 );

        // then
        Assertions.assertEquals( 20, newFile.header().prevFileLastIndex() );
        Assertions.assertEquals( 3, newFile.header().segmentNumber() );
        Assertions.assertEquals( 40, newFile.header().prevIndex() );
        Assertions.assertEquals( 2, newFile.header().prevTerm() );
    }

    @Test
    void shouldRecoverBootstrappedEntry() throws Exception
    {
        for ( int bootstrapIndex = 0; bootstrapIndex < 5; bootstrapIndex++ )
        {
            for ( long bootstrapTerm = 0; bootstrapTerm < 5; bootstrapTerm++ )
            {
                testRecoveryOfBootstrappedEntry( bootstrapIndex, bootstrapTerm );
            }
        }
    }

    private void testRecoveryOfBootstrappedEntry( long bootstrapIndex, long bootstrapTerm )
            throws IOException, DamagedLogStorageException, DisposedException
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, -1, 1, 1, bootstrapIndex, bootstrapTerm );

        RecoveryProtocol protocol =
                new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        // when
        State state = protocol.run();

        // then
        Assertions.assertEquals( bootstrapIndex, state.prevIndex );
        Assertions.assertEquals( bootstrapTerm, state.prevTerm );

        Assertions.assertEquals( -1, state.terms.get( -1 ) );
        Assertions.assertEquals( -1, state.terms.get( bootstrapIndex - 1 ) );
        Assertions.assertEquals( bootstrapTerm, state.terms.get( bootstrapIndex ) );
        Assertions.assertEquals( -1, state.terms.get( bootstrapIndex + 1 ) );

        Assertions.assertEquals( bootstrapTerm, state.terms.latest() );
    }

    @Test
    void shouldRecoverSeveralSkips() throws Exception
    {
        // given
        createLogFile( fsa, 10, 1, 1, 20, 9 );
        createLogFile( fsa, 100, 2, 2, 200, 99 );
        createLogFile( fsa, 1000, 3, 3, 2000, 999 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), INSTANCE );

        // when
        State state = protocol.run();

        // then
        Assertions.assertEquals( 2000, state.prevIndex );
        Assertions.assertEquals( 999, state.prevTerm );

        Assertions.assertEquals( -1, state.terms.get( 20 ) );
        Assertions.assertEquals( -1, state.terms.get( 200 ) );
        Assertions.assertEquals( -1, state.terms.get( 1999 ) );

        Assertions.assertEquals( 999, state.terms.get( 2000 ) );
        Assertions.assertEquals( -1, state.terms.get( 2001 ) );

        Assertions.assertEquals( 999, state.terms.latest() );
    }

    private void createLogFile( FileSystemAbstraction fsa, long prevFileLastIndex, long fileNameVersion,
            long headerVersion, long prevIndex, long prevTerm ) throws IOException
    {
        StoreChannel channel = fsa.write( fileNames.getForSegment( fileNameVersion ) );
        PhysicalFlushableChannel writer = new PhysicalFlushableChannel( channel, INSTANCE );
        headerMarshal.marshal( new SegmentHeader( prevFileLastIndex, headerVersion, prevIndex, prevTerm ), writer );
        writer.prepareForFlush().flush();
        channel.close();
    }

    private void createEmptyLogFile( FileSystemAbstraction fsa, long fileNameVersion ) throws IOException
    {
        StoreChannel channel = fsa.write( fileNames.getForSegment( fileNameVersion ) );
        channel.close();
    }
}
