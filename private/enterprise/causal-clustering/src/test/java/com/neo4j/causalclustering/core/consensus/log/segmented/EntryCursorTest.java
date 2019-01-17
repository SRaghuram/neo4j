/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class EntryCursorTest
{
    private final FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
    private final File bam = new File( "bam" );
    private final FileNames fileNames = new FileNames( bam );
    private ReaderPool readerPool = new ReaderPool( 0, getInstance(), fileNames, fsa,
            Clocks.fakeClock() );
    private final Segments segments =
            new Segments( fsa, fileNames, readerPool, emptyList(), ignored -> mock( ChannelMarshal.class ), NullLogProvider.getInstance(), -1 );

    {
        fsa.mkdir( bam );
    }

    @After
    public void tearDown() throws Exception
    {
        fsa.close();
    }

    @Test
    public void ifFileExistsButEntryDoesNotExist() throws Exception
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
    public void requestedSegmentHasBeenPruned() throws Exception
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
    public void requestedSegmentHasNotExistedYet() throws Exception
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
