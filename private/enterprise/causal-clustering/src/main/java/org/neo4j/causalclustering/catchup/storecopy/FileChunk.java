/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.util.Objects;

public class FileChunk
{
    static final int HEADER_SIZE = 4;
    static final int MAX_PAYLOAD_SIZE = 8192;

    static final int USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS = -1;

    private final int encodedLength;
    private final ByteBuf payload;

    static FileChunk create( ByteBuf payload, boolean isLast )
    {
        if ( !isLast && payload.readableBytes() != MAX_PAYLOAD_SIZE )
        {
            throw new IllegalArgumentException( "All chunks except for the last must be of max size." );
        }
        int encodedLength = isLast ? payload.readableBytes() : USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS;
        return new FileChunk( encodedLength, payload );
    }

    FileChunk( int encodedLength, ByteBuf payload )
    {
        this.encodedLength = encodedLength;
        this.payload = payload;
    }

    public boolean isLast()
    {
        return encodedLength != USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS;
    }

    ByteBuf payload()
    {
        return payload;
    }

    int encodedLength()
    {
        return encodedLength;
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
        FileChunk fileChunk = (FileChunk) o;
        return encodedLength == fileChunk.encodedLength && ByteBufUtil.equals( payload, fileChunk.payload );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( encodedLength, Objects.hashCode( payload ) );
    }

    @Override
    public String toString()
    {
        return "FileChunk{" + "encodedLength=" + encodedLength + ", payload=" + payload + '}';
    }
}
