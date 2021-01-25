/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

// TODO: Tests for new segment header
class SegmentHeaderTest
{
    private SegmentHeader.Marshal marshal = new SegmentHeader.Marshal();

    @Test
    void shouldWriteAndReadHeader() throws Exception
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
        Assertions.assertEquals( writtenHeader, readHeader );
    }

    @Test
    void shouldThrowExceptionWhenReadingIncompleteHeader() throws Exception
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
            Assertions.fail();
        }
        catch ( EndOfStreamException e )
        {
            // expected
        }
    }
}
