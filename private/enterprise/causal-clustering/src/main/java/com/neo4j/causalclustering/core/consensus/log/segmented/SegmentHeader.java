/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.SafeChannelMarshal;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The header written at the beginning of each segment.
 */
public class SegmentHeader
{
    private static final int LEGACY_VERSION = 1;
    private static final int CURRENT_VERSION = 2;

    /**
     * Starting with Raft version 2 the file begins with a Magic value.
     * This struct contains information about the Magic string and its byte encoding
     * THIS INFORMATION MUST *NEVER* CHANGE
     */
    private static final class Magic
    {
        private Magic()
        {
        }
        static final String STRING = "_NEO4J_RAFT_LOG_";
        static final byte[] BYTES = STRING.getBytes( UTF_8 );
        static final int LENGTH = BYTES.length;
    }

    private static final int RECORD_OFFSET_V1 = 4 /* prevFileLastIndex + segmentNumber + prevIndex + prevTerm */ * Long.BYTES;
    private static final int RECORD_OFFSET_V2 = Magic.STRING.length() + 2 /* formatVersion + recordOffset */ * Integer.BYTES + RECORD_OFFSET_V1;

    static final int CURRENT_RECORD_OFFSET = RECORD_OFFSET_V2;

    private final int formatVersion;
    private final int recordOffset;

    private final long prevFileLastIndex;
    private final long segmentNumber;
    private final long prevIndex;
    private final long prevTerm;

    SegmentHeader( long prevFileLastIndex, long segmentNumber, long prevIndex, long prevTerm )
    {
        this( CURRENT_VERSION, CURRENT_RECORD_OFFSET, prevFileLastIndex, segmentNumber, prevIndex, prevTerm );
    }

    SegmentHeader( int formatVersion, int recordOffset, long prevFileLastIndex, long segmentNumber, long prevIndex, long prevTerm )
    {
        this.formatVersion = formatVersion;
        this.recordOffset = recordOffset;

        this.prevFileLastIndex = prevFileLastIndex;
        this.segmentNumber = segmentNumber;
        this.prevIndex = prevIndex;
        this.prevTerm = prevTerm;
    }

    int formatVersion()
    {
        return formatVersion;
    }

    /**
     * The record offset is where the actual raft log entries begin. This is generally right after the
     * header, but is encoded as a field in the file itself for future extensibility.
     *
     * @return The offset where the records with raft log entries begin.
     */
    long recordOffset()
    {
        return recordOffset;
    }

    long prevFileLastIndex()
    {
        return prevFileLastIndex;
    }

    long segmentNumber()
    {
        return segmentNumber;
    }

    public long prevIndex()
    {
        return prevIndex;
    }

    public long prevTerm()
    {
        return prevTerm;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        SegmentHeader that = (SegmentHeader) o;
        return formatVersion == that.formatVersion && recordOffset == that.recordOffset && prevFileLastIndex == that.prevFileLastIndex &&
                segmentNumber == that.segmentNumber && prevIndex == that.prevIndex && prevTerm == that.prevTerm;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( formatVersion, recordOffset, prevFileLastIndex, segmentNumber, prevIndex, prevTerm );
    }

    @Override
    public String toString()
    {
        return "SegmentHeader{" +
                "formatVersion=" + formatVersion +
                ", recordOffset=" + recordOffset +
                ", prevFileLastIndex=" + prevFileLastIndex +
                ", segmentNumber=" + segmentNumber +
                ", prevIndex=" + prevIndex +
                ", prevTerm=" + prevTerm + '}';
    }

    static class Marshal extends SafeChannelMarshal<SegmentHeader>
    {
        @Override
        public void marshal( SegmentHeader header, WritableChannel channel ) throws IOException
        {
            if ( header.formatVersion < CURRENT_VERSION )
            {
                throw new IllegalArgumentException( format( "This software does not support writing version %s headers.", header.formatVersion ) );
            }

            channel.put( Magic.BYTES, Magic.BYTES.length );
            channel.putInt( header.formatVersion );
            channel.putInt( CURRENT_RECORD_OFFSET );

            channel.putLong( header.prevFileLastIndex );
            channel.putLong( header.segmentNumber );
            channel.putLong( header.prevIndex );
            channel.putLong( header.prevTerm );
        }

        @Override
        public SegmentHeader unmarshal0( ReadableChannel channel ) throws IOException
        {
            byte[] headBytes = new byte[Magic.LENGTH];
            channel.get( headBytes, Magic.LENGTH );
            ByteBuffer headBytesBuffer = ByteBuffer.wrap( headBytes );

            int formatVersion;
            int recordOffset;
            long prevFileLastIndex;
            long segmentNumber;

            if ( Arrays.equals( headBytesBuffer.array(), Magic.BYTES ) )
            {
                formatVersion = channel.getInt();
                recordOffset = channel.getInt();
                ensureValid( formatVersion, recordOffset );

                prevFileLastIndex = channel.getLong();
                segmentNumber = channel.getLong();
            }
            else
            {
                formatVersion = LEGACY_VERSION;
                recordOffset = RECORD_OFFSET_V1;
                prevFileLastIndex = headBytesBuffer.getLong();
                segmentNumber = headBytesBuffer.getLong();
            }

            long prevIndex = channel.getLong();
            long prevTerm = channel.getLong();

            return new SegmentHeader( formatVersion, recordOffset, prevFileLastIndex, segmentNumber, prevIndex, prevTerm );
        }

        private void ensureValid( int formatVersion, int recordOffset )
        {
            if ( formatVersion != CURRENT_VERSION )
            {
                throw new IllegalStateException( format( "Unsupported format version %s", formatVersion ) );
            }
            else if ( recordOffset != CURRENT_RECORD_OFFSET )
            {
                throw new IllegalStateException( "Invalid record offset" );
            }
        }
    }
}
