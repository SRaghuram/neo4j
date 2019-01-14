/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// TODO: Tests for new segment header
public class SegmentHeaderTest
{
    private SegmentHeader.Marshal marshal = new SegmentHeader.Marshal();

    @Test
    public void shouldWriteAndReadHeader() throws Exception
    {
        // given
        long prevFileLastIndex = 1;
        long version = 2;
        long prevIndex = 3;
        long prevTerm = 4;

        SegmentHeader writtenHeader = new SegmentHeader( prevFileLastIndex, version, prevIndex, prevTerm );

        InMemoryClosableChannel channel = new InMemoryClosableChannel();

        // when
        marshal.marshal( writtenHeader, channel );
        SegmentHeader readHeader = marshal.unmarshal( channel );

        // then
        assertEquals( writtenHeader, readHeader );
    }

    @Test
    public void shouldThrowExceptionWhenReadingIncompleteHeader() throws Exception
    {
        // given
        long prevFileLastIndex = 1;
        long version = 2;
        long prevIndex = 3;
        long prevTerm = 4;

        SegmentHeader writtenHeader = new SegmentHeader( prevFileLastIndex, version, prevIndex, prevTerm );
        InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong( writtenHeader.segmentNumber() );
        channel.putLong( writtenHeader.prevIndex() );

        // when
        try
        {
            marshal.unmarshal( channel );
            fail();
        }
        catch ( EndOfStreamException e )
        {
            // expected
        }
    }
}
