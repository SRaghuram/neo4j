/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class EntryCursorTest
{
    private final FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
    private final Path bam = Path.of( "bam" );
    private final FileNames fileNames = new FileNames( bam );
    private final ReaderPool readerPool = new ReaderPool( 0, getInstance(), fileNames, fsa, Clocks.fakeClock() );
    private final Segments segments =
            new Segments( fsa, fileNames, readerPool, emptyList(), ignored -> mock( ChannelMarshal.class ), NullLogProvider.getInstance(), -1, INSTANCE );

    {
        fsa.mkdir( bam );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        fsa.close();
    }

    @Test
    void ifFileExistsButEntryDoesNotExist() throws Exception
    {
        // When
        segments.rotate( -1, -1, -1 );
        segments.rotate( 10, 10, 10 );
        segments.last().closeWriter();

        EntryCursor entryCursor = new EntryCursor( segments, 1L );

        boolean next = entryCursor.next();

        assertFalse( next );
    }

    @Test
    void requestedSegmentHasBeenPruned() throws Exception
    {
        // When
        segments.rotate( -1, -1, -1 );
        segments.rotate( 10, 10, 10 );
        segments.rotate( 20, 20, 20 );
        segments.prune( 12 );
        segments.last().closeWriter();

        EntryCursor entryCursor = new EntryCursor( segments, 1L );

        boolean next = entryCursor.next();

        assertFalse( next );
    }

    @Test
    void requestedSegmentHasNotExistedYet() throws Exception
    {
        // When
        segments.rotate( -1, -1, -1 );
        segments.rotate( 10, 10, 10 );
        segments.rotate( 20, 20, 20 );
        segments.last().closeWriter();

        EntryCursor entryCursor = new EntryCursor( segments, 100L );

        boolean next = entryCursor.next();

        assertFalse( next );
    }
}
